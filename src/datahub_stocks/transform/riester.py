"""Riester pension contract as one position of the portfolio.

The contract is not priced like an ETF: its current value is maintained by hand
(like an account balance), the contributions come from the `Riester Buy` sheet
of the trades file and the fees from the Raisin fee export. No more
contributions are paid in at the moment, but later ones are picked up as well.

The fees are taken out of the contract (by selling parts of it), so the
current value is already net of them: unlike an ETF, whose order costs are paid
on top, the gain is `value - total_investment`, and `total_cost` only reports
the fees that were charged.
"""

import logging

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)

RIESTER_ID = "RIESTER"
RIESTER_NAME = "Riester (Raisin)"
RIESTER_SECURITY_TYPE = "Riester"
RIESTER_DEPOT = "raisin"

FEE_COLUMN_MAP = {
    "Datum": "date",
    "Buchungsart": "booking_type",
    "Betrag in EUR": "fee",
    "Bemerkung": "comment",
}
# The export ends with a total row (`Datum` = "Summe")
FEE_TOTAL_LABEL = "Summe"

VALUE_COLUMN_MAP = {"Datum": "date", "Wert": "value", "Kommentar": "comment"}

PORTFOLIO_VALUE_COLUMNS = [
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
    "depot",
    "security_type",
]
CASHFLOW_COLUMNS = ["date", "isin", "security_type", "type", "amount"]


def _check_columns(data: pd.DataFrame, column_map: dict, table: str) -> None:
    missing = set(column_map) - set(data.columns)
    assert not missing, f"Columns missing in `{table}`: {sorted(missing)}"


def preprocess_riester_fees(data: pd.DataFrame) -> pd.DataFrame:
    """Normalises the Raisin Riester fee export to [date, fee, comment].

    Fees are positive, refunds (`Korrektur ... (Erstattung)`) negative. The
    trailing total row is dropped after checking that it matches the bookings,
    so a truncated or edited export doesn't go unnoticed."""
    _check_columns(data, FEE_COLUMN_MAP, "Riester fees")
    fees = data.rename(columns=FEE_COLUMN_MAP)[list(FEE_COLUMN_MAP.values())].copy()
    fees["fee"] = pd.to_numeric(fees["fee"], errors="raise").astype(float)

    is_total = fees["date"].astype(str).str.strip() == FEE_TOTAL_LABEL
    bookings = fees[~is_total].dropna(subset=["date"]).copy()
    bookings["date"] = pd.to_datetime(bookings["date"], format="%d.%m.%Y")
    if is_total.any():
        total = fees.loc[is_total, "fee"].iloc[0]
        assert np.isclose(bookings["fee"].sum(), total), (
            f"Riester fees sum up to {bookings['fee'].sum():.2f} EUR, but the "
            f"export's total row says {total:.2f} EUR"
        )
    return bookings[["date", "fee", "comment"]].reset_index(drop=True)


def preprocess_riester_values(data: pd.DataFrame) -> pd.DataFrame:
    """Normalises the manually maintained contract values to [date, value]."""
    _check_columns(data, {"Datum": None, "Wert": None}, "Riester value")
    values = data.rename(columns=VALUE_COLUMN_MAP)
    values = values.dropna(subset=["date", "value"]).copy()
    if not pd.api.types.is_datetime64_any_dtype(values["date"]):
        values["date"] = pd.to_datetime(values["date"], format="%d.%m.%Y")
    values["value"] = pd.to_numeric(values["value"], errors="raise").astype(float)
    assert not values["date"].duplicated().any(), "Duplicated date in Riester value"
    return values[["date", "value"]].sort_values("date").reset_index(drop=True)


def calculate_riester_value(
    contributions: pd.DataFrame, fees: pd.DataFrame, values: pd.DataFrame
) -> pd.DataFrame:
    """The contract as one row of the portfolio value table: the latest
    recorded value, all contributions as `total_investment` and all fees as
    `total_cost`. Empty when no value has been recorded yet."""
    if values.empty:
        logger.warning("No Riester value recorded yet; the contract is left out")
        return pd.DataFrame(columns=PORTFOLIO_VALUE_COLUMNS)
    latest = values.iloc[-1]
    invested = float(contributions["amount"].sum())
    gained = latest["value"] - invested
    row = {
        # a timestamp like the ETF/stock rows, so the combined date column
        # keeps one type (mixed types are written with a time part)
        "date": latest["date"],
        "isin": RIESTER_ID,
        "name": RIESTER_NAME,
        "cumulative_shares": np.nan,
        "price": np.nan,
        "value": latest["value"],
        "total_cost": float(fees["fee"].sum()),
        "total_investment": invested,
        "value_gained": gained,
        "value_gained_pct": gained / invested * 100 if invested else np.nan,
        "symbol": "",
        "type": RIESTER_SECURITY_TYPE,
        "currency": "EUR",
        "depot": RIESTER_DEPOT,
        "security_type": RIESTER_SECURITY_TYPE,
    }
    return pd.DataFrame([row], columns=PORTFOLIO_VALUE_COLUMNS)


def build_riester_cashflows(
    contributions: pd.DataFrame, riester_value: pd.DataFrame
) -> pd.DataFrame:
    """The contract's cashflows for the money-weighted return: every
    contribution paid in (negative) and the latest value at the date it was
    recorded (positive). Fees are no cashflows of their own: they are paid out
    of the contract and already lower its value."""
    if riester_value.empty:
        return pd.DataFrame(columns=CASHFLOW_COLUMNS)
    paid_in = pd.DataFrame(
        {
            "date": contributions["date"].dt.strftime("%Y-%m-%d"),
            "isin": RIESTER_ID,
            "security_type": RIESTER_SECURITY_TYPE,
            "type": "buy",
            "amount": -contributions["amount"],
        }
    )
    value = riester_value.assign(
        date=pd.to_datetime(riester_value["date"]).dt.strftime("%Y-%m-%d"),
        type="value",
        amount=riester_value["value"],
    )
    return pd.concat([paid_in, value[CASHFLOW_COLUMNS]], ignore_index=True).reset_index(
        drop=True
    )
