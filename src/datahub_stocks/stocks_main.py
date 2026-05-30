import logging
from pathlib import Path

from utils.file_io import save_data, load_data
from datahub_stocks.transform.preprocessing import (
    preprocess_portfolio,
    preprocess_mergers,
    apply_mergers,
    aggregate_monthly_shares,
)
from datahub_stocks.extract.extract_master_data import (
    initialize_master_data,
    extract_current_etf_prices,
    extract_historic_etf_prices,
)

from constants import DATAHUB_ROOT_FILEPATH


logger = logging.getLogger(__name__)


def run_stocks(init: bool = False):
    filepath_source = Path(DATAHUB_ROOT_FILEPATH) / "source" / "stocks"
    filepath_target = Path(DATAHUB_ROOT_FILEPATH) / "target" / "stocks"

    # Load source-data
    etf_portfolio = load_data(
        filepath_source / "source_stocks_portfolio_trades.ods",
        file_type="excel",
        sheet_name="Buys",
    )
    etf_portfolio = preprocess_portfolio(etf_portfolio)
    etf_mergers = load_data(
        filepath_source / "source_stock_mergers.ods",
        file_type="excel",
    )
    etf_mergers = preprocess_mergers(etf_mergers)
    logger.info("Portfolio Data loaded!")

    # etf_portfolio = apply_mergers(etf_portfolio, etf_mergers)
    etf_shares = aggregate_monthly_shares(etf_portfolio)
    save_data(etf_shares, filepath_target / "etf_shares_monthly.csv")
    logger.info("Monthly share holdings computed and saved!")

    etf_isin_valid = list(set(etf_portfolio["isin"].str.strip()))

    # Extract: Stocks Datahub
    path_master_data = filepath_target / "source_master_data.csv"
    if init:
        initialize_master_data(
            etf_isins=etf_isin_valid,
            out_path=path_master_data,
        )
        logger.info(f"Initialized masterdata in {path_master_data}!")
    master_data = load_data(
        filepath=path_master_data, used_library="pandas", file_type="csv"
    )

    path_current_prices = filepath_target / "source_etf_price_current.csv"
    etf_current_prices = extract_current_etf_prices(
        etfs=master_data[["isin", "symbol", "currency"]],
    )
    save_data(
        data=etf_current_prices,
        filepath=path_current_prices,
    )
    logger.info(f"Updated current price data in {path_current_prices}!")

    path_historic_prices = filepath_target / "source_etf_price_historic.csv"
    etf_historic_prices = extract_historic_etf_prices(
        etfs=master_data[["isin", "symbol", "currency"]].dropna(subset=["symbol"]),
    )
    save_data(
        data=etf_historic_prices,
        filepath=path_historic_prices,
    )
    logger.info(f"Updated historic price data in {path_historic_prices}!")


if __name__ == "__main__":
    run_stocks()
