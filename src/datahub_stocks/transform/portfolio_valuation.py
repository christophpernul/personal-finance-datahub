"""Calculates the current market value of the ETF portfolio."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def calculate_portfolio_value(
    etf_portfolio: pd.DataFrame,
    etf_current_prices: pd.DataFrame,
    etf_isin_valid: list,
) -> pd.DataFrame:
    """Calculates the current value of each ETF position in the portfolio.

    For each valid ISIN, sums up all bought amounts, looks up the current
    price, and computes position value = total_amount * current_price.

    :param etf_portfolio: Buy history with columns [isin, amount, cost, name, ...].
    :param etf_current_prices: Current prices with columns [isin, date, price].
    :param etf_isin_valid: List of valid ISINs (merged/converted ones excluded).
    :return: DataFrame with per-position and total portfolio valuation.
    """
    portfolio = etf_portfolio[etf_portfolio["isin"].isin(etf_isin_valid)].copy()

    holdings = (
        portfolio.groupby("isin")
        .agg(total_amount=("amount", "sum"), total_cost=("cost", "sum"))
        .reset_index()
    )

    names = portfolio.drop_duplicates(subset="isin", keep="last")[["isin", "name"]]
    holdings = holdings.merge(names, on="isin", how="left")

    prices = etf_current_prices[["isin", "price"]].rename(
        columns={"price": "current_price"}
    )
    holdings = holdings.merge(prices, on="isin", how="left")

    holdings["current_value"] = holdings["total_amount"] * holdings["current_price"]
    holdings["value_gained"] = holdings["current_value"] - holdings["total_cost"]
    holdings["value_gained_pct"] = (
        holdings["value_gained"] / holdings["total_cost"]
    ) * 100

    holdings = holdings.sort_values("current_value", ascending=False).reset_index(
        drop=True
    )

    total_value = holdings["current_value"].sum()
    total_cost = holdings["total_cost"].sum()
    logger.info(
        f"Portfolio value: {total_value:,.2f} EUR "
        f"(invested: {total_cost:,.2f} EUR, "
        f"value gained: {total_value - total_cost:,.2f} EUR, "
        f"{((total_value - total_cost) / total_cost) * 100:.1f}%)"
    )

    return holdings[
        [
            "isin",
            "name",
            "total_amount",
            "total_cost",
            "current_price",
            "current_value",
            "value_gained",
            "value_gained_pct",
        ]
    ]
