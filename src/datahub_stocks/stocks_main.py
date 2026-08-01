import logging

import pandas as pd

from utils.file_io import save_data, load_data
from datahub_stocks.transform.preprocessing import (
    preprocess_buys,
    preprocess_sells,
    preprocess_mergers,
    preprocess_master_data,
    preprocess_dividends,
    apply_mergers,
    aggregate_monthly_shares,
    aggregate_monthly_investments,
    aggregate_monthly_dividends,
    calculate_portfolio_value,
)
from datahub_stocks.extract.extract_master_data import (
    extract_current_etf_prices,
    extract_historic_etf_prices,
    initialize_market_snapshot,
)

from constants import (
    PATH_STOCKS_TRADES,
    PATH_SINGLE_STOCKS_TRADES,
    PATH_STOCK_MERGERS,
    PATH_ETF_MASTER_DATA,
    PATH_ETF_MARKET_SNAPSHOT,
    PATH_ETF_PRICE_CURRENT,
    PATH_ETF_PRICE_HISTORIC,
    PATH_ETF_SHARES_MONTHLY,
    PATH_ETF_MONTHLY_INVESTMENTS,
    PATH_ETF_MONTHLY_DIVIDENDS,
    PATH_ETF_PORTFOLIO_VALUE,
)


logger = logging.getLogger(__name__)


def run_stocks():
    # Load source-data
    etf_buys = load_data(
        PATH_STOCKS_TRADES,
        file_type="excel",
        sheet_name="Buys",
    )

    etf_buys = preprocess_buys(etf_buys)
    etf_sells = load_data(
        PATH_STOCKS_TRADES,
        file_type="excel",
        sheet_name="Sells",
    )
    etf_sells = preprocess_sells(etf_sells)
    etf_portfolio = pd.concat([etf_buys, etf_sells], ignore_index=True)

    # Received dividends: ETF distributions and individual stock dividends,
    # combined into a single table (both sheets share the same layout).
    etf_dividends = load_data(
        PATH_STOCKS_TRADES,
        file_type="excel",
        sheet_name="Dividends",
    )
    etf_dividends = preprocess_dividends(etf_dividends)
    stock_dividends = load_data(
        PATH_SINGLE_STOCKS_TRADES,
        file_type="excel",
        sheet_name="Dividends",
    )
    stock_dividends = preprocess_dividends(stock_dividends)
    dividends = pd.concat([etf_dividends, stock_dividends], ignore_index=True)
    logger.info("Dividends Data (ETF and stocks) loaded!")

    etf_mergers = load_data(
        PATH_STOCK_MERGERS,
        file_type="excel",
    )
    etf_mergers = preprocess_mergers(etf_mergers)
    logger.info("Mergers Data loaded!")

    ### EXTRACT

    # Load ETF master data (regenerated separately via init_master_data.py)
    master_data = load_data(
        filepath=PATH_ETF_MASTER_DATA, used_library="pandas", file_type="csv"
    )
    initialize_market_snapshot(
        etf_isins=master_data["isin"].dropna().unique().tolist(),
        out_path=PATH_ETF_MARKET_SNAPSHOT,
    )
    master_data = preprocess_master_data(master_data)
    logger.info("ETF Master Data loaded and preprocessed!")

    # Extract current ETF price data
    etf_current_prices = extract_current_etf_prices(
        etfs=master_data[["isin", "symbol", "currency"]],
    )
    save_data(
        data=etf_current_prices,
        filepath=PATH_ETF_PRICE_CURRENT,
    )
    logger.info(f"Updated current price data in {PATH_ETF_PRICE_CURRENT}!")

    # Extract historic ETF price data
    etf_historic_prices = extract_historic_etf_prices(
        etfs=master_data[["isin", "symbol", "currency"]].dropna(subset=["symbol"]),
    )
    save_data(
        data=etf_historic_prices,
        filepath=PATH_ETF_PRICE_HISTORIC,
    )
    logger.info(f"Updated historic price data in {PATH_ETF_PRICE_HISTORIC}!")

    ### TRANSFORM

    # Transform monthly portfolio history to cumulative share holdings per month
    etf_portfolio = apply_mergers(etf_portfolio, etf_mergers)
    etf_shares = aggregate_monthly_shares(etf_portfolio)
    save_data(etf_shares, PATH_ETF_SHARES_MONTHLY)
    logger.info("Monthly share holdings computed and saved!")

    # Monthly net invested amount and order costs
    monthly_investments = aggregate_monthly_investments(etf_portfolio)
    save_data(monthly_investments, PATH_ETF_MONTHLY_INVESTMENTS)
    logger.info("Monthly investments and order costs computed and saved!")

    # Monthly received dividends per security (ETFs and individual stocks)
    monthly_dividends = aggregate_monthly_dividends(dividends)
    save_data(monthly_dividends, PATH_ETF_MONTHLY_DIVIDENDS)
    logger.info("Monthly dividends per security computed and saved!")

    # Calculate current portfolio value
    portfolio_value = calculate_portfolio_value(
        etf_shares, etf_current_prices, master_data, etf_portfolio
    )
    save_data(portfolio_value, PATH_ETF_PORTFOLIO_VALUE)

    return portfolio_value, monthly_investments, monthly_dividends


if __name__ == "__main__":
    run_stocks()
