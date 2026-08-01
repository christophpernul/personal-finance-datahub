import pandas as pd
import pytest

from src.datahub_stocks.transform.preprocessing import (
    apply_mergers,
    aggregate_monthly_shares,
    aggregate_monthly_investments,
    preprocess_dividends,
    aggregate_monthly_dividends,
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


@pytest.mark.ut
def test_aggregate_monthly_investments_sums_per_month():
    portfolio = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-15", "2024-01-20", "2024-03-10"]),
            "trade_type": ["buy", "buy", "buy"],
            "total_investment": [100.0, 50.0, 200.0],
            "cost": [1.0, 0.5, 2.0],
        }
    )
    result = aggregate_monthly_investments(portfolio)
    assert result.columns.tolist() == [
        "date",
        "expense_investment",
        "income_investment",
        "order_costs",
    ]
    jan = result[result["date"] == "2024-01-31"].iloc[0]
    assert jan["expense_investment"] == 150.0
    assert jan["income_investment"] == 0.0
    assert jan["order_costs"] == 1.5
    mar = result[result["date"] == "2024-03-31"].iloc[0]
    assert mar["expense_investment"] == 200.0
    assert mar["order_costs"] == 2.0


@pytest.mark.ut
def test_aggregate_monthly_investments_splits_buys_and_sells():
    portfolio = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-05", "2024-02-25"]),
            "trade_type": ["buy", "sell"],
            # buys are positive, sells negative in total_investment
            "total_investment": [300.0, -100.0],
            "cost": [1.0, 2.0],
        }
    )
    result = aggregate_monthly_investments(portfolio)
    feb = result[result["date"] == "2024-02-29"].iloc[0]
    assert feb["expense_investment"] == 300.0  # only the buy
    assert feb["income_investment"] == 100.0  # sell proceeds, positive
    assert feb["order_costs"] == 3.0  # order costs of both


@pytest.fixture
def dividends_raw():
    # mirrors the German-headed Dividends sheet layout
    return pd.DataFrame(
        {
            "Datum": ["15.1.2024", "20.1.2024", "10.3.2024"],
            "Betrag": [1.5, 2.5, 4.0],
            "Name": ["ETF A", "ETF B", "ETF A"],
            "ISIN": ["A", "B", "A"],
        }
    )


@pytest.mark.ut
def test_preprocess_dividends_renames_and_types(dividends_raw):
    result = preprocess_dividends(dividends_raw)
    assert result.columns.tolist() == ["date", "isin", "name", "dividend"]
    assert pd.api.types.is_datetime64_any_dtype(result["date"])
    assert result["dividend"].sum() == 8.0


@pytest.mark.ut
def test_aggregate_monthly_dividends_sums_per_month_and_isin(dividends_raw):
    result = aggregate_monthly_dividends(preprocess_dividends(dividends_raw))
    assert result.columns.tolist() == ["date", "isin", "name", "dividend"]
    jan_a = result[(result["date"] == "2024-01-31") & (result["isin"] == "A")].iloc[0]
    assert jan_a["dividend"] == 1.5
    jan_b = result[(result["date"] == "2024-01-31") & (result["isin"] == "B")].iloc[0]
    assert jan_b["dividend"] == 2.5
    mar_a = result[(result["date"] == "2024-03-31") & (result["isin"] == "A")].iloc[0]
    assert mar_a["dividend"] == 4.0
    # one row per (month, isin); no all-zero rows
    assert len(result) == 3
