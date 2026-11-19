import pandas as pd
import pytest

from src.datahub_stocks.transform.earnings import EARNINGS_COLUMNS, build_earnings


@pytest.fixture
def dividends():
    # as from preprocess_dividends: individual stocks carry no ISIN
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-03-15", "2024-06-15", "2025-06-15", "2024-04-01", "2024-05-01"]
            ),
            "isin": ["IE00X", "IE00X", "IE00X", "", ""],
            "name": [
                "Old ETF name",
                "New ETF name",
                "New ETF name",
                "Coca Cola",
                "Coca Cola",
            ],
            "dividend": [10.0, 12.5, 0.0, 1.5, 1.6],
        }
    )


@pytest.fixture
def interest():
    # as from preprocess_interest
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-01", "2024-12-31"]),
            "category": ["Tagesgeld", "Festgeld"],
            "isin": ["ZT0002", "ZF0101"],
            "account": ["DKB Tagesgeld", "WeltSparen Festgeld"],
            "interest": [3.2, 80.4],
        }
    )


@pytest.mark.ut
def test_build_earnings_columns_and_order(dividends, interest):
    result = build_earnings(dividends, interest)
    assert result.columns.tolist() == EARNINGS_COLUMNS
    assert result["date"].tolist() == sorted(result["date"])
    # the zero dividend is dropped
    assert len(result) == 6
    assert result["amount"].sum() == pytest.approx(10.0 + 12.5 + 1.5 + 1.6 + 3.2 + 80.4)


@pytest.mark.ut
def test_build_earnings_categorises_and_names_holdings(dividends, interest):
    result = build_earnings(dividends, interest)
    by_holding = result.drop_duplicates("holding").set_index("holding")
    # ETFs are keyed by ISIN and keep their latest name
    assert by_holding.loc["IE00X", "category"] == "ETF"
    assert by_holding.loc["IE00X", "name"] == "New ETF name"
    # stocks have no ISIN and are keyed by name
    assert by_holding.loc["Coca Cola", "category"] == "Aktie"
    assert by_holding.loc["Coca Cola", "kind"] == "dividend"
    # interest accounts are keyed by account id
    assert by_holding.loc["ZF0101", "category"] == "Festgeld"
    assert by_holding.loc["ZF0101", "name"] == "WeltSparen Festgeld"
    assert by_holding.loc["ZT0002", "kind"] == "interest"


@pytest.mark.ut
def test_build_earnings_without_interest(dividends, interest):
    result = build_earnings(dividends, interest.iloc[0:0])
    assert set(result["kind"]) == {"dividend"}
