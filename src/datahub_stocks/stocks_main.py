import logging
from pathlib import Path

import pandas as pd

from utils.file_io import save_data, load_data
from datahub_stocks.transform.preprocessing import (
    preprocess_buys,
    preprocess_sells,
    preprocess_mergers,
    preprocess_master_data,
    apply_mergers,
    aggregate_monthly_shares,
    calculate_portfolio_value,
)
from datahub_stocks.extract.extract_master_data import (
    extract_current_etf_prices,
    extract_historic_etf_prices,
    initialize_market_snapshot,
)

from constants import DATAHUB_ROOT_FILEPATH


logger = logging.getLogger(__name__)


def run_stocks():
    filepath_source = Path(DATAHUB_ROOT_FILEPATH) / "source" / "stocks"
    filepath_target = Path(DATAHUB_ROOT_FILEPATH) / "target" / "stocks"

    # Load source-data
    etf_buys = load_data(
        filepath_source / "source_stocks_portfolio_trades.ods",
        file_type="excel",
        sheet_name="Buys",
    )

    etf_buys = preprocess_buys(etf_buys)
    etf_sells = load_data(
        filepath_source / "source_stocks_portfolio_trades.ods",
        file_type="excel",
        sheet_name="Sells",
    )
    etf_sells = preprocess_sells(etf_sells)
    etf_portfolio = pd.concat([etf_buys, etf_sells], ignore_index=True)

    etf_mergers = load_data(
        filepath_source / "source_stock_mergers.ods",
        file_type="excel",
    )
    etf_mergers = preprocess_mergers(etf_mergers)
    logger.info("Mergers Data loaded!")

    ### EXTRACT

    # Load ETF master data (regenerated separately via init_master_data.py)
    path_master_data = filepath_target / "source_master_data.csv"
    master_data = load_data(
        filepath=path_master_data, used_library="pandas", file_type="csv"
    )
    initialize_market_snapshot(
        etf_isins=master_data["isin"].dropna().unique().tolist(),
        out_path=filepath_target / "master_data_market_snapshot.csv",
    )
    master_data = preprocess_master_data(master_data)
    logger.info("ETF Master Data loaded and preprocessed!")

    # Extract current ETF price data
    path_current_prices = filepath_target / "source_etf_price_current.csv"
    etf_current_prices = extract_current_etf_prices(
        etfs=master_data[["isin", "symbol", "currency"]],
    )
    save_data(
        data=etf_current_prices,
        filepath=path_current_prices,
    )
    logger.info(f"Updated current price data in {path_current_prices}!")

    # Extract historic ETF price data
    path_historic_prices = filepath_target / "source_etf_price_historic.csv"
    etf_historic_prices = extract_historic_etf_prices(
        etfs=master_data[["isin", "symbol", "currency"]].dropna(subset=["symbol"]),
    )
    save_data(
        data=etf_historic_prices,
        filepath=path_historic_prices,
    )
    logger.info(f"Updated historic price data in {path_historic_prices}!")

    ### TRANSFORM

    # Transform monthly portfolio history to cumulative share holdings per month
    etf_portfolio = apply_mergers(etf_portfolio, etf_mergers)
    etf_shares = aggregate_monthly_shares(etf_portfolio)
    save_data(etf_shares, filepath_target / "etf_shares_monthly.csv")
    logger.info("Monthly share holdings computed and saved!")

    # Calculate current portfolio value
    portfolio_value = calculate_portfolio_value(
        etf_shares, etf_current_prices, master_data, etf_portfolio
    )
    save_data(portfolio_value, filepath_target / "portfolio_value.csv")


if __name__ == "__main__":
    run_stocks()
