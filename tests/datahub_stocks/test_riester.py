import numpy as np
import pandas as pd
import pytest

from src.datahub_stocks.transform.riester import (
    CASHFLOW_COLUMNS,
    PORTFOLIO_VALUE_COLUMNS,
    RIESTER_ID,
    build_riester_cashflows,
    calculate_riester_value,
    preprocess_riester_fees,
    preprocess_riester_values,
)


def _fee_export(total="27,90"):
    """Raw fee export as read with decimal="," (amounts already numeric)."""
    rows = [
        ("25.11.2019", "Gebührenbelastung", 7.90, np.nan),
        ("17.06.2020", "Gebührenbelastung", 18.00, np.nan),
        ("04.11.2020", "Gebührenbelastung", 6.27, np.nan),
        ("25.11.2020", "Korrektur Gebührenbelastung (Erstattung)", -6.27, "Rück"),
        ("03.12.2020", "Gebührenbelastung", 2.00, np.nan),
    ]
    if total is not None:
        rows.append(("Summe", np.nan, float(total.replace(",", ".")), "Netto"))
    return pd.DataFrame(
        rows, columns=["Datum", "Buchungsart", "Betrag in EUR", "Bemerkung"]
    )


@pytest.fixture
def contributions():
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2019-11-18", "2019-11-22", "2019-12-02"]),
            "amount": [165.0, 1607.9, 160.0],
        }
    )


@pytest.fixture
def values():
    return preprocess_riester_values(
        pd.DataFrame(
            {
                "Datum": pd.to_datetime(["2026-11-13", "2025-12-31"]),
                "Wert": [2500.0, 2300.0],
                "Kommentar": [np.nan, "alt"],
            }
        )
    )


@pytest.mark.ut
def test_preprocess_riester_fees_drops_total_and_keeps_refunds():
    fees = preprocess_riester_fees(_fee_export())
    assert len(fees) == 5
    assert fees["fee"].sum() == pytest.approx(27.90)
    assert fees["date"].iloc[0] == pd.Timestamp("2019-11-25")


@pytest.mark.ut
def test_preprocess_riester_fees_checks_the_total_row():
    with pytest.raises(AssertionError, match="total row"):
        preprocess_riester_fees(_fee_export(total="30,00"))
    # an export without a total row is fine as well
    assert len(preprocess_riester_fees(_fee_export(total=None))) == 5


@pytest.mark.ut
def test_preprocess_riester_values_sorts_and_parses_text_dates():
    values = preprocess_riester_values(
        pd.DataFrame(
            {
                "Datum": ["13.11.2026", "31.12.2025", np.nan],
                "Wert": [2500.0, 2300.0, np.nan],
            }
        )
    )
    assert list(values["value"]) == [2300.0, 2500.0]


@pytest.mark.ut
def test_calculate_riester_value_uses_latest_value(contributions, values):
    fees = preprocess_riester_fees(_fee_export())
    row = calculate_riester_value(contributions, fees, values)

    assert list(row.columns) == PORTFOLIO_VALUE_COLUMNS
    row = row.iloc[0]
    assert row["isin"] == RIESTER_ID
    assert row["security_type"] == "Riester"
    assert row["date"] == pd.Timestamp("2026-11-13")
    assert row["value"] == 2500.0
    assert row["total_investment"] == pytest.approx(1932.9)
    assert row["total_cost"] == pytest.approx(27.90)
    # fees are already paid out of the contract: not subtracted again
    assert row["value_gained"] == pytest.approx(2500.0 - 1932.9)
    assert row["value_gained_pct"] == pytest.approx((2500.0 - 1932.9) / 1932.9 * 100)


@pytest.mark.ut
def test_calculate_riester_value_without_value_is_empty(contributions):
    fees = preprocess_riester_fees(_fee_export())
    empty = preprocess_riester_values(pd.DataFrame({"Datum": [], "Wert": []}))
    row = calculate_riester_value(contributions, fees, empty)
    assert row.empty
    assert build_riester_cashflows(contributions, row).empty


@pytest.mark.ut
def test_build_riester_cashflows(contributions, values):
    fees = preprocess_riester_fees(_fee_export())
    flows = build_riester_cashflows(
        contributions, calculate_riester_value(contributions, fees, values)
    )
    assert list(flows.columns) == CASHFLOW_COLUMNS
    assert list(flows["type"]) == ["buy", "buy", "buy", "value"]
    assert flows["amount"].sum() == pytest.approx(2500.0 - 1932.9)
    assert flows["date"].iloc[-1] == "2026-11-13"
    assert (flows["isin"] == RIESTER_ID).all()
