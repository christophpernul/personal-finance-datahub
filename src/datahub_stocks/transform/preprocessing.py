"""Contains preprocessing functionalities for stock and ETF data."""

import logging

import pandas as pd

from utils.datacleaning import convert_columns_to_timestamp


logger = logging.getLogger(__name__)


PORTFOLIO_REQUIRED_COLUMNS = {
    "index",
    "date",
    "type",
    "price",
    "amount",
    "cost",
    "depot",
    "comment",
    "name",
    "isin",
    "note",
}
MERGERS_REQUIRED_COLUMNS = {
    "isin_old",
    "isin_new",
    "name_old",
    "name_new",
    "type",
    "stocks_old",
    "stocks_new",
    "date",
}
MASTER_DATA_REQUIRED_COLUMNS = {
    "isin",
    "name",
    "symbol",
    "type",
    "currency",
    "distribution",
    "replication",
    "ter",
    "region",
    "etf_type",
    "comment",
}


def _validate_columns(data: pd.DataFrame, required: set, table_name: str) -> None:
    missing = required - set(data.columns)
    assert not missing, f"Columns missing in `{table_name}`: {missing}"


def preprocess_portfolio(data: pd.DataFrame) -> pd.DataFrame:
    _validate_columns(data, PORTFOLIO_REQUIRED_COLUMNS, "portfolio")
    if not pd.api.types.is_datetime64_any_dtype(data["date"]):
        data = convert_columns_to_timestamp(data, column_formats={"date": "%d.%m.%Y"})
    data["shares"] = -data["amount"] / data["price"]  # Amount is a cost and is negative
    return data


def preprocess_mergers(data: pd.DataFrame) -> pd.DataFrame:
    _validate_columns(data, MERGERS_REQUIRED_COLUMNS, "mergers")
    if not pd.api.types.is_datetime64_any_dtype(data["date"]):
        data = convert_columns_to_timestamp(data, column_formats={"date": "%d.%m.%Y"})
    return data


def preprocess_master_data(data: pd.DataFrame) -> pd.DataFrame:
    _validate_columns(data, MASTER_DATA_REQUIRED_COLUMNS, "master_data")
    return data


def apply_mergers(portfolio: pd.DataFrame, mergers: pd.DataFrame) -> pd.DataFrame:
    """Replaces old (merged) ETF ISINs in the portfolio with their successor ISINs
    and adjusts share counts by the conversion ratio stocks_new / stocks_old."""
    result = portfolio.copy()
    for _, merger in mergers.iterrows():
        mask = result["isin"] == merger["isin_old"]
        if not mask.any():
            continue
        ratio = merger["stocks_new"] / merger["stocks_old"]
        result.loc[mask, "isin"] = merger["isin_new"]
        result.loc[mask, "shares"] = result.loc[mask, "shares"] * ratio
    return result


def aggregate_monthly_shares(portfolio: pd.DataFrame) -> pd.DataFrame:
    """Aggregates shares per ISIN by month and computes cumulative holdings over time."""
    portfolio = portfolio.copy()
    monthly = (
        portfolio.groupby([pd.Grouper(key="date", freq="ME"), "isin"])["shares"]
        .sum()
        .unstack(fill_value=0)
    )
    full_range = pd.date_range(monthly.index.min(), monthly.index.max(), freq="ME")
    monthly = monthly.reindex(full_range, fill_value=0)
    monthly.index.name = "date"
    result = monthly.cumsum().stack().reset_index()
    result.columns = ["date", "isin", "cumulative_shares"]
    result = result[result["cumulative_shares"] > 0]
    return result


def calculate_portfolio_value(
    shares: pd.DataFrame,
    prices: pd.DataFrame,
    master_data: pd.DataFrame,
    portfolio: pd.DataFrame,
) -> pd.DataFrame:
    """Joins the latest cumulative holdings with current prices and master data,
    and computes gain/loss against the total invested cost per position."""
    latest_date = shares["date"].max()
    current_holdings = shares[shares["date"] == latest_date].copy()
    total_costs = (
        portfolio.groupby("isin", as_index=False)["cost"]
        .sum()
        .rename(columns={"cost": "total_cost"})
    )
    result = current_holdings.merge(
        master_data[
            [
                "isin",
                "name",
                "symbol",
                "type",
                "currency",
                "ter",
            ]
        ],
        on="isin",
        how="left",
    )
    result = result.merge(prices[["isin", "price"]], on="isin", how="left")
    result = result.merge(total_costs, on="isin", how="left")
    result["value"] = result["cumulative_shares"] * result["price"]
    result["value_gained"] = result["value"] - result["total_cost"]
    result["value_gained_pct"] = (result["value_gained"] / result["total_cost"]) * 100

    total_value = result["value"].sum()
    invested = result["total_cost"].sum()
    gained = total_value - invested
    gained_pct = (gained / invested) * 100 if invested else 0.0
    logger.info(
        f"Portfolio value: {total_value:,.2f} EUR "
        f"(invested: {invested:,.2f} EUR, "
        f"gained: {gained:,.2f} EUR, {gained_pct:.1f}%)"
    )

    return result[
        [
            "date",
            "isin",
            "name",
            "symbol",
            "type",
            "currency",
            "cumulative_shares",
            "price",
            "value",
            "total_cost",
            "value_gained",
            "value_gained_pct",
        ]
    ]
