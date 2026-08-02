import logging

import pandas as pd

from utils.file_io import save_data, load_data
from datahub_stocks.transform.preprocessing import (
    preprocess_buys,
    preprocess_sells,
    preprocess_mergers,
    preprocess_master_data,
    preprocess_dividends,
    preprocess_stock_trades,
    preprocess_splits,
    apply_mergers,
    apply_splits,
    aggregate_monthly_shares,
    aggregate_monthly_investments,
    aggregate_monthly_dividends,
    calculate_portfolio_value,
    combine_portfolio_values,
)
from datahub_stocks.extract.extract_master_data import (
    extract_current_etf_prices,
    extract_historic_etf_prices,
    initialize_market_snapshot,
)

from constants import (
    PATH_STOCKS_TRADES,
    PATH_SINGLE_STOCKS_TRADES,
    PATH_STOCKS_MASTER_DATA,
    PATH_STOCK_MERGERS,
    PATH_STOCK_SPLITS,
    PATH_ETF_MASTER_DATA,
    PATH_ETF_MARKET_SNAPSHOT,
    PATH_ETF_PRICE_CURRENT,
    PATH_ETF_PRICE_HISTORIC,
    PATH_ETF_SHARES_MONTHLY,
    PATH_ETF_MONTHLY_INVESTMENTS,
    PATH_ETF_MONTHLY_DIVIDENDS,
    PATH_ETF_PORTFOLIO_VALUE,
    PATH_STOCKS_SHARES_MONTHLY,
    PATH_STOCKS_PORTFOLIO_VALUE,
    PATH_PORTFOLIO_VALUE_COMBINED,
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

    # Individual stock trades (same file/layout as the ETF trades, but German
    # headers, no ISIN and no explicit shares column on the Sells sheet).
    stock_buys = preprocess_stock_trades(
        load_data(PATH_SINGLE_STOCKS_TRADES, file_type="excel", sheet_name="Buys"),
        is_buy=True,
    )
    stock_sells = preprocess_stock_trades(
        load_data(PATH_SINGLE_STOCKS_TRADES, file_type="excel", sheet_name="Sells"),
        is_buy=False,
    )
    stock_trades = pd.concat([stock_buys, stock_sells], ignore_index=True)
    # Only actual stock positions (``Aktien``) form the portfolio; other trade
    # kinds such as an expired ``Put Option`` are treated as a pure investment
    # expense (added to the cashflow below) rather than a holding.
    stock_portfolio = stock_trades[stock_trades["type"] == "Aktien"].copy()
    stock_non_portfolio = stock_trades[stock_trades["type"] != "Aktien"].copy()
    logger.info("Individual stock trades loaded!")

    # Stock splits (e.g. Tesla 1-to-3 on 25.8.2022): shares bought before the
    # split date are re-based into post-split units.
    stock_splits = preprocess_splits(load_data(PATH_STOCK_SPLITS, file_type="excel"))
    logger.info("Stock splits loaded!")

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
    # TODO: Store these snapshots to create a history, also do not save data inside the function!
    initialize_market_snapshot(
        etf_isins=master_data["isin"].dropna().unique().tolist(),
        out_path=PATH_ETF_MARKET_SNAPSHOT,
    )
    master_data = preprocess_master_data(master_data)
    logger.info("ETF Master Data loaded and preprocessed!")

    # Extract current ETF price data
    # TODO: Store these snapshots to create a history
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

    # Load individual-stock master data and extract current prices. Stocks carry
    # no ISIN, so the security name is used as the identifier (`isin` column).
    stock_master = load_data(
        filepath=PATH_STOCKS_MASTER_DATA, used_library="pandas", file_type="csv"
    )
    stock_master["isin"] = stock_master["name"]
    stock_current_prices = extract_current_etf_prices(
        etfs=stock_master[["isin", "symbol", "currency"]],
    )
    logger.info("Individual stock master data loaded and current prices fetched!")

    ### TRANSFORM

    # Transform monthly portfolio history to cumulative share holdings per month
    etf_portfolio = apply_mergers(etf_portfolio, etf_mergers)
    etf_shares = aggregate_monthly_shares(etf_portfolio)
    save_data(etf_shares, PATH_ETF_SHARES_MONTHLY)
    logger.info("Monthly ETF share holdings computed and saved!")

    # Same for the individual stock portfolio (identified by name), after
    # re-basing pre-split shares into post-split units.
    stock_portfolio = apply_splits(stock_portfolio, stock_splits)
    stock_shares = aggregate_monthly_shares(stock_portfolio)
    save_data(stock_shares, PATH_STOCKS_SHARES_MONTHLY)
    logger.info("Monthly stock share holdings computed and saved!")

    # Monthly net invested amount and order costs (ETF trades)
    monthly_investments = aggregate_monthly_investments(etf_portfolio)
    save_data(monthly_investments, PATH_ETF_MONTHLY_INVESTMENTS)
    logger.info("Monthly investments and order costs computed and saved!")

    # Non-portfolio stock trades (e.g. the expired Put Option) are recorded as an
    # additional investment expense in the cashflow, so fold them into the
    # monthly investments handed to the target stage (buys -> expense_investment).
    if not stock_non_portfolio.empty:
        other_investments = aggregate_monthly_investments(stock_non_portfolio)
        monthly_investments = (
            pd.concat([monthly_investments, other_investments], ignore_index=True)
            .groupby("date", as_index=False)[
                ["expense_investment", "income_investment", "order_costs"]
            ]
            .sum()
        )
        logger.info("Non-portfolio stock trades added as investment expenses!")

    # Monthly received dividends per security (ETFs and individual stocks)
    monthly_dividends = aggregate_monthly_dividends(dividends)
    save_data(monthly_dividends, PATH_ETF_MONTHLY_DIVIDENDS)
    logger.info("Monthly dividends per security computed and saved!")

    # Calculate current portfolio value (ETFs and individual stocks)
    portfolio_value = calculate_portfolio_value(
        etf_shares, etf_current_prices, master_data, etf_portfolio
    )
    save_data(portfolio_value, PATH_ETF_PORTFOLIO_VALUE)

    stock_portfolio_value = calculate_portfolio_value(
        stock_shares, stock_current_prices, stock_master, stock_portfolio
    )
    save_data(stock_portfolio_value, PATH_STOCKS_PORTFOLIO_VALUE)
    logger.info("Stock portfolio value computed and saved!")

    # Single combined table with a security_type of ETF or Aktie
    combined_portfolio_value = combine_portfolio_values(
        portfolio_value, stock_portfolio_value
    )
    save_data(combined_portfolio_value, PATH_PORTFOLIO_VALUE_COMBINED)
    logger.info(
        f"Combined ETF + stock portfolio value saved in {PATH_PORTFOLIO_VALUE_COMBINED}"
    )

    return portfolio_value, monthly_investments, monthly_dividends


if __name__ == "__main__":
    run_stocks()
