import pandas as pd
import pytest

from src.datahub_stocks.transform.preprocessing import (
    apply_mergers,
    aggregate_monthly_shares,
)


@pytest.fixture
def portfolio():
    return pd.DataFrame(
        {
            "Datum": pd.to_datetime(
                ["2024-01-15", "2024-02-10", "2024-03-20", "2024-01-05"]
            ),
            "isin": ["OLD1", "OLD1", "NEW1", "KEEP"],
            "shares": [10.0, 5.0, 20.0, 8.0],
        }
    )


@pytest.fixture
def mergers():
    return pd.DataFrame(
        {
            "isin_old": ["OLD1"],
            "isin_new": ["MERGED1"],
            "stocks_old": [2.0],
            "stocks_new": [1.0],
        }
    )


@pytest.mark.ut
def test_apply_mergers_replaces_isin(portfolio, mergers):
    result = apply_mergers(portfolio, mergers)
    assert "OLD1" not in result["isin"].values
    assert (result["isin"] == "MERGED1").sum() == 2


@pytest.mark.ut
def test_apply_mergers_adjusts_shares(portfolio, mergers):
    result = apply_mergers(portfolio, mergers)
    merged_rows = result[result["isin"] == "MERGED1"]
    assert merged_rows["shares"].tolist() == [5.0, 2.5]


@pytest.mark.ut
def test_apply_mergers_keeps_unaffected_rows(portfolio, mergers):
    result = apply_mergers(portfolio, mergers)
    keep_row = result[result["isin"] == "KEEP"]
    assert keep_row["shares"].iloc[0] == 8.0
    new_row = result[result["isin"] == "NEW1"]
    assert new_row["shares"].iloc[0] == 20.0


@pytest.mark.ut
def test_apply_mergers_no_matching_isin(portfolio):
    empty_mergers = pd.DataFrame(
        {
            "isin_old": ["NONEXISTENT"],
            "isin_new": ["WHATEVER"],
            "stocks_old": [1.0],
            "stocks_new": [1.0],
        }
    )
    result = apply_mergers(portfolio, empty_mergers)
    pd.testing.assert_frame_equal(result, portfolio)


@pytest.mark.ut
def test_aggregate_monthly_shares_cumulates():
    portfolio = pd.DataFrame(
        {
            "Datum": pd.to_datetime(["2024-01-15", "2024-01-20", "2024-03-10"]),
            "isin": ["A", "A", "A"],
            "shares": [10.0, 5.0, 3.0],
        }
    )
    result = aggregate_monthly_shares(portfolio)
    a_rows = result[result["isin"] == "A"].sort_values("date")
    cumulative = a_rows["cumulative_shares"].tolist()
    assert cumulative[0] == 15.0
    assert cumulative[-1] == 18.0


@pytest.mark.ut
def test_aggregate_monthly_shares_fills_gaps():
    portfolio = pd.DataFrame(
        {
            "Datum": pd.to_datetime(["2024-01-15", "2024-03-10"]),
            "isin": ["A", "A"],
            "shares": [10.0, 5.0],
        }
    )
    result = aggregate_monthly_shares(portfolio)
    a_rows = result[result["isin"] == "A"].sort_values("date")
    assert len(a_rows) == 3
    assert a_rows["cumulative_shares"].tolist() == [10.0, 10.0, 15.0]
