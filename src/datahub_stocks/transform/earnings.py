"""Earnings: every dividend and every interest payment in one table.

Dividends (ETF distributions and individual stock dividends) and interest on
cash deposits (Tagesgeld and Festgeld) are what the money earns without being
sold. This module puts them side by side, one row per payment, so the dashboard
can sum them per year and break each year down by holding:

- ``kind``: ``dividend`` or ``interest``
- ``category``: ``ETF`` / ``Aktie`` for dividends (an individual stock carries
  no ISIN), ``Tagesgeld`` / ``Festgeld`` for interest
- ``holding``: the ISIN of an ETF, the name of a stock or the account id of an
  interest account; ``name`` is its display name
- ``amount``: the positive amount received
"""

import pandas as pd

EARNINGS_COLUMNS = ["date", "kind", "category", "holding", "name", "amount"]
KIND_DIVIDEND = "dividend"
KIND_INTEREST = "interest"
CATEGORY_ETF = "ETF"
CATEGORY_STOCK = "Aktie"


def _latest_name(names: pd.Series, holdings: pd.Series, dates: pd.Series) -> pd.Series:
    """The most recent name of each holding, so a renamed security keeps one
    name across all of its payments."""
    frame = pd.DataFrame({"holding": holdings, "name": names, "date": dates})
    latest = frame.sort_values("date", kind="stable").groupby("holding")["name"].last()
    return holdings.map(latest)


def build_earnings(dividends: pd.DataFrame, interest: pd.DataFrame) -> pd.DataFrame:
    """Combine received dividends and interest into one table of payments.

    ``dividends``: [date, isin, name, dividend] as from ``preprocess_dividends``
    (``isin`` is empty for individual stocks).
    ``interest``: [date, category, isin, account, interest] as from
    ``preprocess_interest``.

    Returns ``EARNINGS_COLUMNS`` sorted by date, without zero or missing amounts.
    """
    isin = dividends["isin"].fillna("").astype(str).str.strip()
    is_etf = isin != ""
    div_holding = isin.where(is_etf, dividends["name"])
    div = pd.DataFrame(
        {
            "date": pd.to_datetime(dividends["date"]),
            "kind": KIND_DIVIDEND,
            "category": is_etf.map({True: CATEGORY_ETF, False: CATEGORY_STOCK}),
            "holding": div_holding,
            "name": _latest_name(dividends["name"], div_holding, dividends["date"]),
            "amount": pd.to_numeric(dividends["dividend"], errors="coerce"),
        }
    )
    intr = pd.DataFrame(
        {
            "date": pd.to_datetime(interest["date"]),
            "kind": KIND_INTEREST,
            "category": interest["category"],
            "holding": interest["isin"],
            "name": _latest_name(
                interest["account"], interest["isin"], interest["date"]
            ),
            "amount": pd.to_numeric(interest["interest"], errors="coerce"),
        }
    )
    earnings = pd.concat([div, intr], ignore_index=True)
    earnings = earnings[earnings["amount"].notna() & (earnings["amount"] != 0.0)]
    earnings = earnings.sort_values(["date", "kind", "holding"], kind="stable")
    earnings["date"] = earnings["date"].dt.strftime("%Y-%m-%d")
    return earnings[EARNINGS_COLUMNS].reset_index(drop=True)
