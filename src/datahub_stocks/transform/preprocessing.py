"""Contains preprocessing functionalities for stock and ETF data."""

import logging

import pandas as pd

from utils.datacleaning import convert_columns_to_timestamp, strip_vals


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
SELLS_REQUIRED_COLUMNS = {
    "index",
    "date",
    "type",
    "price",
    "amount",
    "cost",
    "depot",
    "shares",
    "name",
    "isin",
    "_checkSharesEqualAmountDivPrice",
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
DIVIDENDS_REQUIRED_COLUMNS = {
    "Datum",
    "Betrag",
    "Name",
    "ISIN",
}
STOCK_TRADES_REQUIRED_COLUMNS = {
    "Datum",
    "Art",
    "Kurs",
    "Betrag",
    "Kosten",
    "Name",
}
SPLITS_REQUIRED_COLUMNS = {
    "name",
    "date",
    "split_from",
    "split_to",
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


def preprocess_buys(data: pd.DataFrame) -> pd.DataFrame:
    _validate_columns(data, PORTFOLIO_REQUIRED_COLUMNS, "portfolio")

    if not pd.api.types.is_datetime64_any_dtype(data["date"]):
        data = convert_columns_to_timestamp(data, column_formats={"date": "%d.%m.%Y"})

    float_cols = ["price", "amount", "cost"]
    data[float_cols] = data[float_cols].apply(pd.to_numeric, errors="coerce")

    str_cols = [
        "depot",
        "comment",
        "name",
        "isin",
        "note",
        "type",
    ]
    data = strip_vals(data, str_cols)

    data["amount"] *= -1  # Amount is a cost and is negative
    data["cost"] *= -1
    data["shares"] = data["amount"] / data["price"]
    data["trade_type"] = "buy"
    data = data.rename(columns={"amount": "total_investment"}).drop("price", axis=1)
    return data


def preprocess_sells(data: pd.DataFrame) -> pd.DataFrame:
    """Normalises the Sells sheet such that it can be concatenated with Buys before
    aggregation: shares and amount are negated so cumulative sums reduce holdings.
    """
    _validate_columns(data, SELLS_REQUIRED_COLUMNS, "sells")
    if not pd.api.types.is_datetime64_any_dtype(data["date"]):
        data = convert_columns_to_timestamp(data, column_formats={"date": "%d.%m.%Y"})

    # When sold the number of shares and the total amount invested decreases
    data["shares"] = -data["shares"]
    data["amount"] = -data["amount"]
    data["trade_type"] = "sell"
    data = data.rename(columns={"amount": "total_investment"}).drop("price", axis=1)
    return data


def preprocess_mergers(data: pd.DataFrame) -> pd.DataFrame:
    _validate_columns(data, MERGERS_REQUIRED_COLUMNS, "mergers")
    if not pd.api.types.is_datetime64_any_dtype(data["date"]):
        data = convert_columns_to_timestamp(data, column_formats={"date": "%d.%m.%Y"})

    str_cols = [
        "isin_new",
        "isin_old",
        "name_old",
        "name_new",
        "type",
    ]
    data = strip_vals(data, str_cols)

    for col in ["stocks_old", "stocks_new"]:
        data[col] = pd.to_numeric(
            data[col].astype(str).str.strip().str.replace(",", ".", regex=False)
        )
    return data


def preprocess_stock_trades(data: pd.DataFrame, is_buy: bool) -> pd.DataFrame:
    """Normalises an individual-stock ``Buys``/``Sells`` sheet.

    The stock trade sheets use German headers and, unlike the ETF sheets, carry
    no ISIN (securities are identified by name) and no explicit ``shares`` column
    on the Sells sheet. To reuse the ISIN-keyed portfolio aggregation unchanged,
    the security `name` is used as the `isin` identifier.

    Sign convention (matching the ETF portfolio after preprocessing): `shares`
    and `total_investment` are positive for buys and negative for sells, so a
    cumulative sum reduces holdings on a sell; `cost` (order fees) stays positive
    for both. The original trade kind (``Art``, e.g. ``Aktien``/``Put Option``)
    is kept in the `type` column so non-stock rows can be filtered out later.
    """
    _validate_columns(data, STOCK_TRADES_REQUIRED_COLUMNS, "stock_trades")
    data = data.rename(
        columns={
            "Datum": "date",
            "Art": "type",
            "Kurs": "price",
            "Betrag": "amount",
            "Kosten": "cost",
            "Name": "name",
        }
    )

    if not pd.api.types.is_datetime64_any_dtype(data["date"]):
        data = convert_columns_to_timestamp(data, column_formats={"date": "%d.%m.%Y"})

    float_cols = ["price", "amount", "cost"]
    data[float_cols] = data[float_cols].apply(pd.to_numeric, errors="coerce")
    data = strip_vals(data, ["name", "type"])

    # Stocks carry no ISIN; use the name as the security identifier.
    data["isin"] = data["name"]
    # Stocks are only traded in whole shares; the raw amount/price ratio only
    # deviates from an integer because of price rounding, so round to full shares.
    data["shares"] = (data["amount"] / data["price"]).round()
    if is_buy:
        data["trade_type"] = "buy"
    else:
        # Sells reduce holdings and the net invested amount.
        data["shares"] = -data["shares"]
        data["amount"] = -data["amount"]
        data["trade_type"] = "sell"
    data = data.rename(columns={"amount": "total_investment"}).drop("price", axis=1)
    return data[
        [
            "date",
            "isin",
            "name",
            "type",
            "total_investment",
            "cost",
            "shares",
            "trade_type",
        ]
    ]


def preprocess_dividends(data: pd.DataFrame) -> pd.DataFrame:
    """Normalises a Dividends sheet (received ETF distributions or stock dividends).

    The sheet uses German column headers, so the relevant columns are renamed to
    the datahub's English convention and reduced to [date, isin, name, dividend].
    `dividend` (``Betrag``) is the distribution amount received and is positive.
    Individual stocks carry no ISIN; the missing ISIN is normalised to an empty
    string so those securities are identified by `name` alone.
    """
    _validate_columns(data, DIVIDENDS_REQUIRED_COLUMNS, "dividends")
    data = data.rename(
        columns={
            "Datum": "date",
            "Betrag": "dividend",
            "Name": "name",
            "ISIN": "isin",
        }
    )[["date", "isin", "name", "dividend"]]

    if not pd.api.types.is_datetime64_any_dtype(data["date"]):
        data = convert_columns_to_timestamp(data, column_formats={"date": "%d.%m.%Y"})

    data["dividend"] = pd.to_numeric(data["dividend"], errors="coerce")
    # ISIN is absent for individual stocks (a fully empty float column); represent
    # it as a stripped string ("" when missing) so it stays a valid group key.
    data["isin"] = data["isin"].fillna("").astype(str).str.strip()
    data = strip_vals(data, ["name"])
    return data


def aggregate_monthly_dividends(dividends: pd.DataFrame) -> pd.DataFrame:
    """Aggregates received dividends (ETF distributions and stock dividends) per
    security by month.

    Returns a long-format table with one row per (month, security) holding the
    summed `dividend` amount, where a security is identified by [isin, name]
    (ISIN is empty for individual stocks). Dates are the month-end (last day of
    the month). Months without any distribution for a security are not included."""
    dividends = dividends.copy()
    monthly = (
        dividends.groupby([pd.Grouper(key="date", freq="ME"), "isin", "name"])[
            "dividend"
        ]
        .sum()
        .reset_index()
    )
    monthly = monthly[monthly["dividend"] != 0.0]
    monthly["date"] = monthly["date"].dt.strftime("%Y-%m-%d")
    return monthly[["date", "isin", "name", "dividend"]]


def preprocess_splits(data: pd.DataFrame) -> pd.DataFrame:
    """Normalises the stock splits table (keyed by security name).

    Each row describes a split for one security: `split_from` shares became
    `split_to` shares on `date` (e.g. a 1-to-3 split has split_from=1,
    split_to=3)."""
    _validate_columns(data, SPLITS_REQUIRED_COLUMNS, "splits")
    if not pd.api.types.is_datetime64_any_dtype(data["date"]):
        data = convert_columns_to_timestamp(data, column_formats={"date": "%d.%m.%Y"})

    for col in ["split_from", "split_to"]:
        data[col] = pd.to_numeric(data[col])
    data = strip_vals(data, ["name"])
    return data


def apply_splits(portfolio: pd.DataFrame, splits: pd.DataFrame) -> pd.DataFrame:
    """Adjusts the share counts of trades that happened *before* a split date by
    the split ratio ``split_to / split_from``.

    Only shares acquired before the split are re-based into post-split units;
    trades on or after the split date are already quoted in post-split shares and
    are left unchanged. The invested amount and order costs are not touched."""
    result = portfolio.copy()
    for _, split in splits.iterrows():
        ratio = split["split_to"] / split["split_from"]
        mask = (result["name"] == split["name"]) & (result["date"] < split["date"])
        if not mask.any():
            continue
        result.loc[mask, "shares"] = result.loc[mask, "shares"] * ratio
    return result


def preprocess_master_data(data: pd.DataFrame) -> pd.DataFrame:
    _validate_columns(data, MASTER_DATA_REQUIRED_COLUMNS, "master_data")

    #     Do not strip columns that have no string data
    all_cols = list(
        MASTER_DATA_REQUIRED_COLUMNS.difference(
            {"ter", "replication", "etf_type", "comment"}
        )
    )
    data = strip_vals(data, all_cols)
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


def aggregate_monthly_investments(portfolio: pd.DataFrame) -> pd.DataFrame:
    """Aggregates monthly buy expenses, sell income and order costs per month.

    `total_investment` is positive for buys and negative for sells, so buys map
    to `expense_investment` (money invested) and sells to `income_investment`
    (proceeds, reported as a positive amount). `cost` holds the order fees
    (positive for both buys and sells) and is summed into `order_costs`. Dates
    are the month-end (last day of the month)."""
    portfolio = portfolio.copy()
    is_buy = portfolio["trade_type"] == "buy"
    portfolio["expense_investment"] = portfolio["total_investment"].where(is_buy, 0.0)
    portfolio["income_investment"] = (-portfolio["total_investment"]).where(
        ~is_buy, 0.0
    )
    monthly = (
        portfolio.groupby(pd.Grouper(key="date", freq="ME"))[
            ["expense_investment", "income_investment", "cost"]
        ]
        .sum()
        .reset_index()
        .rename(columns={"cost": "order_costs"})
    )
    monthly["date"] = monthly["date"].dt.strftime("%Y-%m-%d")
    return monthly[["date", "expense_investment", "income_investment", "order_costs"]]


def combine_portfolio_values(
    etf_value: pd.DataFrame, stock_value: pd.DataFrame
) -> pd.DataFrame:
    """Concatenates the ETF and individual-stock portfolio value tables into a
    single table, tagging each row with a `security_type` of ``ETF`` or
    ``Aktie`` so both asset classes can be reported together."""
    etf = etf_value.copy()
    stock = stock_value.copy()
    etf["security_type"] = "ETF"
    stock["security_type"] = "Aktie"
    return pd.concat([etf, stock], ignore_index=True)


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
        portfolio.groupby("isin", as_index=False)[["cost", "total_investment"]]
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
    result["value_gained"] = (
        result["value"] - result["total_investment"] - result["total_cost"]
    )
    result["value_gained_pct"] = (
        result["value_gained"] / result["total_investment"]
    ) * 100

    total_value = result["value"].sum()
    invested = result["total_investment"].sum()
    total_costs = result["total_cost"].sum()
    gained = total_value - invested - total_costs
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
            "cumulative_shares",
            "price",
            "value",
            "total_cost",
            "total_investment",
            "value_gained",
            "value_gained_pct",
            "symbol",
            "type",
            "currency",
        ]
    ]
