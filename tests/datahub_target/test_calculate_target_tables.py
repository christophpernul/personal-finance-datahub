import pandas as pd
import pytest

from src.datahub_target.transform.calculate_target_tables import (
    TRANSACTION_COLUMNS,
    map_tags_to_categories,
    build_investment_transactions,
    build_rebalancing_transactions,
    build_dividend_transactions,
    build_interest_transactions,
    build_riester_transactions,
    load_investment_cost_transactions,
    aggregate_transactions_monthly,
    transform_cashflow_to_wide_format,
)


@pytest.mark.ut
def test_map_tags_to_categories_maps_every_tag():
    transactions = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-03", "2020-02-14", "2020-03-02"]),
            "tag": ["groceries", "Lebensmittel", "rent"],
            "amount": [-12.0, -8.0, -900.0],
        }
    )
    tag_category_map = {
        "Lebensmittel": ["Lebensmittel", "groceries"],
        "Home": ["rent"],
    }
    result = map_tags_to_categories(transactions, tag_category_map)
    assert list(result.columns) == TRANSACTION_COLUMNS
    assert result["category"].tolist() == ["Lebensmittel", "Lebensmittel", "Home"]
    # the original tag is kept alongside the category
    assert result["tag"].tolist() == ["groceries", "Lebensmittel", "rent"]


@pytest.mark.ut
def test_map_tags_to_categories_rejects_unknown_tag():
    transactions = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-03"]),
            "tag": ["brand new tag"],
            "amount": [-12.0],
        }
    )
    with pytest.raises(AssertionError, match="not yet categorized"):
        map_tags_to_categories(transactions, {"Home": ["rent"]})


@pytest.mark.ut
def test_map_tags_to_categories_keeps_category_named_like_its_tag():
    transactions = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-03"]),
            "tag": ["Kaution"],
            "amount": [-1500.0],
        }
    )
    result = map_tags_to_categories(transactions, {"Kaution": ["Kaution"]})
    assert result["category"].tolist() == ["Kaution"]


@pytest.fixture
def investment_trades():
    # transaction-level trades: total_investment is positive for buys (money
    # spent) and negative for sells (proceeds), cost holds the order fees
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-10", "2020-02-20", "2020-03-05"]),
            "total_investment": [175.0, -50.0, 400.0],
            "cost": [2.0, 0.0, 1.5],
            "trade_type": ["buy", "sell", "buy"],
        }
    )


@pytest.mark.ut
def test_build_investment_transactions_signs_buys_and_sells(investment_trades):
    result = build_investment_transactions(investment_trades)
    assert list(result.columns) == TRANSACTION_COLUMNS

    investments = result[result["tag"] == "Investment"]
    # buys become negative outflows, the sell a positive inflow
    assert investments["amount"].tolist() == [-175.0, 50.0, -400.0]
    assert (investments["category"] == "Investment").all()
    # the trade date is kept, not the month-end
    assert investments["date"].tolist() == list(investment_trades["date"])


@pytest.mark.ut
def test_build_investment_transactions_book_order_costs_separately(investment_trades):
    result = build_investment_transactions(investment_trades)
    order_costs = result[result["tag"] == "order_costs"]
    # order fees are negated, and the fee-free sell produces no row at all
    assert order_costs["amount"].tolist() == [-2.0, -1.5]
    assert (order_costs["category"] == "order_costs").all()


@pytest.fixture
def monthly_rebalancing():
    # date stored as string (yyyy-MM-dd, month-end), as written to the CSV;
    # Feb is a net income month, Mar a net expense month, Apr a no-op
    return pd.DataFrame(
        {
            "date": ["2020-02-29", "2020-03-31", "2020-04-30"],
            "expense_rebalancing": [0.0, 400.0, 0.0],
            "income_rebalancing": [50.0, 0.0, 0.0],
        }
    )


@pytest.mark.ut
def test_build_rebalancing_transactions_net_per_month(monthly_rebalancing):
    result = build_rebalancing_transactions(monthly_rebalancing)
    # one month-end row per month, a net of zero drops out entirely
    assert result["date"].tolist() == list(pd.to_datetime(["2020-02-29", "2020-03-31"]))
    assert result["amount"].tolist() == [50.0, -400.0]
    # folded into Investment, but still recognisable by its tag
    assert (result["category"] == "Investment").all()
    assert (result["tag"] == "Rebalancing").all()


@pytest.mark.ut
def test_build_dividend_transactions_keep_payout_date():
    dividends = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-14", "2020-02-28", "2020-03-16"]),
            "isin": ["A", "B", "A"],
            "name": ["ETF A", "ETF B", "ETF A"],
            "dividend": [10.0, 5.0, 8.0],
        }
    )
    result = build_dividend_transactions(dividends)
    # one row per payout, not one per month
    assert result["amount"].tolist() == [10.0, 5.0, 8.0]
    assert result["date"].tolist() == list(dividends["date"])
    assert (result["category"] == "Dividends").all()


@pytest.mark.ut
def test_build_interest_transactions_categorise_by_deposit_kind():
    interest = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-14", "2020-02-28", "2020-03-16"]),
            "category": ["Tagesgeld", "Festgeld", "Tagesgeld"],
            "interest": [10.0, 5.0, 8.0],
        }
    )
    result = build_interest_transactions(interest)
    assert result["category"].tolist() == ["Tagesgeld", "Festgeld", "Tagesgeld"]
    assert result["amount"].tolist() == [10.0, 5.0, 8.0]
    # interest is received money and stays a positive inflow
    assert (result["amount"] > 0).all()


@pytest.mark.ut
def test_build_riester_transactions_are_negated():
    riester = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-01", "2020-03-01"]),
            "amount": [160.0, 166.0],
        }
    )
    result = build_riester_transactions(riester)
    # contributions stored as negative outflows, one row per contribution
    assert result["amount"].tolist() == [-160.0, -166.0]
    assert (result["category"] == "Riester").all()


@pytest.mark.ut
def test_load_investment_cost_transactions_negates_each_cost(tmp_path):
    # all costs are stored as positive amounts (regardless of provider); a
    # negative stored cost is a credit that flips to a positive inflow.
    csv = (
        "Datum,Kosten,Anbieter,Kommentar,Name,ISIN,Note\n"
        "31.12.2020,3.52,comdirect,Wertpapierkosten 2020,ETF A,ISIN1,WKN a\n"
        "31.12.2020,-57.35,comdirect,Wertpapierkosten 2020,EUWAX,ISIN2,WKN b\n"
        "30.04.2022,35.4,traderepublic,TER 2021 alle ETFs,TER 2021,,\n"
        "20.01.2023,0.0,traderepublic,keine Kosten,nichts,,\n"
    )
    path = tmp_path / "comdirect_costs_combined.csv"
    path.write_text(csv, encoding="utf-8")

    result = load_investment_cost_transactions(path)

    # each cost stays its own transaction on its own date, and is negated; the
    # zero-cost row is dropped
    assert result["date"].tolist() == list(
        pd.to_datetime(["2020-12-31", "2020-12-31", "2022-04-30"])
    )
    assert result["amount"].tolist() == pytest.approx([-3.52, 57.35, -35.4])
    assert (result["category"] == "Investment Costs").all()


@pytest.mark.ut
def test_aggregate_transactions_monthly_sums_per_category():
    transactions = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-03", "2020-02-14", "2020-03-02"]),
            "tag": ["groceries", "Lebensmittel", "rent"],
            "category": ["Lebensmittel", "Lebensmittel", "Home"],
            "amount": [-12.0, -8.0, -900.0],
        }
    )
    result = aggregate_transactions_monthly(transactions)
    assert list(result.columns) == ["date", "category", "amount"]
    # both February rows collapse into a single month-end total
    assert result["date"].tolist() == list(pd.to_datetime(["2020-02-29", "2020-03-31"]))
    assert result["amount"].tolist() == [-20.0, -900.0]


@pytest.mark.ut
def test_transform_cashflow_to_wide_format_pivots_categories():
    monthly = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-29", "2020-02-29", "2020-03-31"]),
            "category": ["Lebensmittel", "Home", "Home"],
            "amount": [-20.0, -900.0, -900.0],
        }
    )
    result = transform_cashflow_to_wide_format(monthly)
    assert result["Home"].tolist() == [-900.0, -900.0]
    # a month without any booking in a category is filled with zero
    assert result["Lebensmittel"].tolist() == [-20.0, 0.0]
    assert result["date"].tolist() == list(pd.to_datetime(["2020-02-29", "2020-03-31"]))


@pytest.mark.ut
def test_transform_cashflow_to_wide_format_drops_all_zero_categories():
    monthly = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-29", "2020-03-31"]),
            "category": ["Kaution", "Kaution"],
            "amount": [-1500.0, 1500.0],
        }
    )
    result = transform_cashflow_to_wide_format(monthly)
    # the category nets to zero over the whole period and is therefore dropped
    assert "Kaution" not in result.columns
