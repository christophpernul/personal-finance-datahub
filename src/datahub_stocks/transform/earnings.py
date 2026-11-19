"""Earnings: every dividend and every interest payment in one table.

Dividends (ETF distributions and individual stock dividends) and interest on
cash deposits (Tagesgeld and Festgeld) are what the money earns without being
sold. This module puts them side by side, one row per payment, so the dashboard
can sum them per year and break each year down by holding:

- ``kind``: ``dividend`` or ``interest``
- ``category``: ``ETF`` / ``Aktie`` for dividends (an individual stock carries
  no ISIN), ``Tagesgeld`` / ``Festgeld`` for interest
- ``holding``: the ISIN of an ETF, the name of a stock, the `Vertrags-ID` of
  the fixed-term deposit that paid the interest (when its code in ``Zinsen`` is
  ``ZF_<Vertrags-ID>``) or else the code of the interest account; ``name`` is
  its display name, for a deposit its name in the cash balance sheet
- ``amount``: the positive amount received
"""

import pandas as pd

EARNINGS_COLUMNS = ["date", "kind", "category", "holding", "name", "amount"]
KIND_DIVIDEND = "dividend"
KIND_INTEREST = "interest"
CATEGORY_ETF = "ETF"
CATEGORY_STOCK = "Aktie"
# An interest code (``ISIN`` of the Zinsen sheet) of this prefix plus a
# `Vertrags-ID` of the Festgeld sheet links the payment to that deposit.
DEPOSIT_CODE_PREFIX = "ZF_"


def _latest_name(names: pd.Series, holdings: pd.Series, dates: pd.Series) -> pd.Series:
    """The most recent name of each holding, so a renamed security keeps one
    name across all of its payments."""
    frame = pd.DataFrame({"holding": holdings, "name": names, "date": dates})
    latest = frame.sort_values("date", kind="stable").groupby("holding")["name"].last()
    return holdings.map(latest)


def _unique_names(names: pd.Series, holdings: pd.Series) -> pd.Series:
    """Append the holding id to a name that several holdings share (e.g. two
    fixed-term deposits at the same provider), so each keeps its own name."""
    pairs = pd.DataFrame({"name": names, "holding": holdings})
    shared = pairs.drop_duplicates().groupby("name")["holding"].nunique() > 1
    clash = names.map(shared).fillna(False).astype(bool)
    return names.where(~clash, names + " (" + holdings + ")")


def _interest_holdings(
    interest: pd.DataFrame, deposits: pd.DataFrame | None
) -> tuple[pd.Series, pd.Series]:
    """Holding id and name per interest payment: the fixed-term deposit when
    its code is ``ZF_<Vertrags-ID>``, else the interest account."""
    holding = interest["isin"].astype(str)
    name = interest["account"]
    linked = holding.str.startswith(DEPOSIT_CODE_PREFIX)
    if not linked.any():
        return holding, name
    deposit_id = holding.str[len(DEPOSIT_CODE_PREFIX) :]
    known = (
        deposits.set_index("deposit_id")["name"].fillna("")
        if deposits is not None
        else pd.Series(dtype=str)
    )
    unknown = sorted(set(deposit_id[linked]) - set(known.index))
    assert not unknown, (
        f"Interest codes `{DEPOSIT_CODE_PREFIX}<Vertrags-ID>` in the Zinsen sheet "
        f"whose `Vertrags-ID` is not in the Festgeld sheet of the cash balance "
        f"sheet: {unknown}"
    )
    deposit_name = deposit_id.map(known)
    deposit_name = deposit_name.where(deposit_name != "", deposit_id)
    return holding.where(~linked, deposit_id), name.where(~linked, deposit_name)


def build_earnings(
    dividends: pd.DataFrame,
    interest: pd.DataFrame,
    deposits: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Combine received dividends and interest into one table of payments.

    ``dividends``: [date, isin, name, dividend] as from ``preprocess_dividends``
    (``isin`` is empty for individual stocks).
    ``interest``: [date, category, isin, account, interest] as from
    ``preprocess_interest``.
    ``deposits``: the fixed-term deposits as from ``preprocess_term_deposits``
    ([deposit_id, name, ...]); needed when an interest code names a deposit
    (``ZF_<Vertrags-ID>``).

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
    int_holding, int_name = _interest_holdings(interest, deposits)
    intr = pd.DataFrame(
        {
            "date": pd.to_datetime(interest["date"]),
            "kind": KIND_INTEREST,
            "category": interest["category"],
            "holding": int_holding,
            "name": _latest_name(int_name, int_holding, interest["date"]),
            "amount": pd.to_numeric(interest["interest"], errors="coerce"),
        }
    )
    earnings = pd.concat([div, intr], ignore_index=True)
    earnings["name"] = _unique_names(earnings["name"], earnings["holding"])
    earnings = earnings[earnings["amount"].notna() & (earnings["amount"] != 0.0)]
    earnings = earnings.sort_values(["date", "kind", "holding"], kind="stable")
    earnings["date"] = earnings["date"].dt.strftime("%Y-%m-%d")
    return earnings[EARNINGS_COLUMNS].reset_index(drop=True)
