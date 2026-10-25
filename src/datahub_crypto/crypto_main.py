import logging
import warnings
from pathlib import Path
from typing import Callable

import pandas as pd

from utils.file_io import save_data, load_data, load_manual_transactions
from datahub_crypto.transform.preprocessing import (
    combine_export_files,
    preprocess_kraken_ledgers,
    preprocess_binance_trades,
    preprocess_binance_deposits,
    preprocess_kucoin_trades,
    preprocess_kucoin_deposits,
    preprocess_manual_transactions,
    combine_ledgers,
    apply_conversions,
    extract_fiat_transfers,
)
from datahub_crypto.transform.portfolio import (
    calculate_cost_basis,
    calculate_current_holdings,
    check_holdings,
    aggregate_monthly_holdings,
    calculate_coin_overview,
    calculate_crypto_portfolio_value,
)
from datahub_stocks.extract.extract_master_data import extract_current_etf_prices
from constants import (
    CRYPTO_INPUT_DIR,
    CRYPTO_KRAKEN_LEDGERS_FILEPATTERN,
    CRYPTO_BINANCE_TRADES_FILEPATTERN,
    CRYPTO_BINANCE_DEPOSITS_FILEPATTERN,
    CRYPTO_KUCOIN_TRADES_FILEPATTERN,
    CRYPTO_KUCOIN_DEPOSITS_FILEPATTERN,
    PATH_MANUAL_TRANSACTIONS,
    MANUAL_ACTIONS,
    MANUAL_ACTION_CRYPTO_TRANSACTION,
    MANUAL_ACTION_CRYPTO_CONVERSION,
    PATH_CRYPTO_MASTER_DATA,
    PATH_CRYPTO_LEDGER,
    PATH_CRYPTO_TRADES,
    PATH_CRYPTO_HOLDINGS_MONTHLY,
    PATH_CRYPTO_PORTFOLIO_VALUE,
    PATH_CRYPTO_COINS,
)


logger = logging.getLogger(__name__)


def _read_csv_export(path: Path) -> pd.DataFrame:
    # Binance and KuCoin exports start with a byte order mark
    return pd.read_csv(path, encoding="utf-8-sig")


def _read_excel_export(path: Path) -> pd.DataFrame:
    with warnings.catch_warnings():
        # Binance writes workbooks without a default style
        warnings.simplefilter("ignore", UserWarning)
        return pd.read_excel(path, engine="openpyxl")


def _load_exchange_ledger(
    filepattern: str,
    read: Callable[[Path], pd.DataFrame],
    preprocess: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame | None:
    """Reads all exports matching `filepattern` from the crypto source folder,
    drops the rows repeated by overlapping exports and converts them into
    ledger rows."""
    exports = []
    for path in sorted(CRYPTO_INPUT_DIR.glob(filepattern)):
        try:
            exports.append(read(path))
        except pd.errors.EmptyDataError:
            logger.info(f"Skipping empty crypto export {path.name}")
    export = combine_export_files(exports)
    if export.empty:
        logger.warning(f"No crypto exports found for {CRYPTO_INPUT_DIR / filepattern}")
        return None
    logger.info(f"Loaded {len(export)} rows from {len(exports)} `{filepattern}` files")
    return preprocess(export)


def run_crypto() -> pd.DataFrame:
    """Runs the crypto transform stage.

    Returns the EUR deposits to and withdrawals from the exchanges
    ([date, exchange, amount]) so the target stage can book them as cashflow.
    """
    ### LOAD & PREPROCESS: one ledger over all exchanges and wallets
    exchange_ledgers = [
        _load_exchange_ledger(
            CRYPTO_KRAKEN_LEDGERS_FILEPATTERN,
            _read_csv_export,
            preprocess_kraken_ledgers,
        ),
        _load_exchange_ledger(
            CRYPTO_BINANCE_TRADES_FILEPATTERN,
            _read_csv_export,
            preprocess_binance_trades,
        ),
        _load_exchange_ledger(
            CRYPTO_BINANCE_DEPOSITS_FILEPATTERN,
            _read_excel_export,
            preprocess_binance_deposits,
        ),
        _load_exchange_ledger(
            CRYPTO_KUCOIN_TRADES_FILEPATTERN, _read_csv_export, preprocess_kucoin_trades
        ),
        _load_exchange_ledger(
            CRYPTO_KUCOIN_DEPOSITS_FILEPATTERN,
            _read_csv_export,
            preprocess_kucoin_deposits,
        ),
    ]
    # Coin movements missing in the exports (e.g. deposits to a private wallet)
    manual_transactions = preprocess_manual_transactions(
        load_manual_transactions(
            PATH_MANUAL_TRANSACTIONS, MANUAL_ACTION_CRYPTO_TRANSACTION, MANUAL_ACTIONS
        )
    )
    ledger = combine_ledgers(
        [
            exchange_ledger
            for exchange_ledger in exchange_ledgers + [manual_transactions]
            if exchange_ledger is not None
        ]
    )
    # Coins converted by the exchange (e.g. on delisting) are trades, too
    ledger = apply_conversions(
        ledger,
        load_manual_transactions(
            PATH_MANUAL_TRANSACTIONS, MANUAL_ACTION_CRYPTO_CONVERSION, MANUAL_ACTIONS
        ),
    )
    save_data(ledger, PATH_CRYPTO_LEDGER)
    logger.info(f"Crypto ledger with {len(ledger)} bookings saved!")

    ### TRANSFORM
    # Cost basis and fees per coin (average cost over all exchanges)
    trades, coins = calculate_cost_basis(ledger)
    save_data(trades, PATH_CRYPTO_TRADES)
    logger.info(f"{len(trades)} crypto trades with cost basis and fees saved!")

    holdings = calculate_current_holdings(ledger)
    check_holdings(holdings, coins)
    save_data(aggregate_monthly_holdings(ledger), PATH_CRYPTO_HOLDINGS_MONTHLY)
    logger.info("Monthly crypto holdings per exchange computed and saved!")

    ### EXTRACT current prices (yahoo symbols from the master data)
    master_data = load_data(PATH_CRYPTO_MASTER_DATA, file_type="csv")
    prices = extract_current_etf_prices(
        etfs=master_data.rename(columns={"coin": "isin"})[
            ["isin", "symbol", "currency"]
        ],
    ).rename(columns={"isin": "coin"})
    logger.info(f"Current prices fetched for {len(prices)} coins!")

    ### VALUE
    coin_overview = calculate_coin_overview(coins, holdings, prices, master_data)
    save_data(coin_overview, PATH_CRYPTO_COINS)
    portfolio_value = calculate_crypto_portfolio_value(coin_overview, holdings)
    save_data(portfolio_value, PATH_CRYPTO_PORTFOLIO_VALUE)
    logger.info(f"Crypto portfolio value saved in {PATH_CRYPTO_PORTFOLIO_VALUE}")

    # The money moved between bank and exchanges is the crypto cashflow
    return extract_fiat_transfers(ledger)


if __name__ == "__main__":
    run_crypto()
