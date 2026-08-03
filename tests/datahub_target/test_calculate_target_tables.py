import pandas as pd
import pytest

from src.datahub_target.transform.calculate_target_tables import (
    add_investment_income,
    add_dividend_income,
    add_interest_income,
    add_investment_expenses,
    add_investment_costs,
    add_rebalancing_income,
    add_rebalancing_expenses,
    load_investment_costs,
)


@pytest.fixture
def monthly_investments():
    # date stored as string (yyyy-MM-dd, month-end), as written to the CSV
    return pd.DataFrame(
        {
            "date": ["2020-02-29", "2020-03-31"],
            "expense_investment": [175.0, 400.0],
            "income_investment": [50.0, 0.0],
            "order_costs": [2.0, 1.5],
        }
    )


@pytest.mark.ut
def test_add_investment_income_matches_on_month(monthly_investments):
    incomes_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-29", "2020-03-31"]),
            "Salary": [1000.0, 1000.0],
        }
    )
    result = add_investment_income(incomes_wide, monthly_investments)
    assert result["Investment"].tolist() == [50.0, 0.0]
    # income proceeds keep their positive sign, matching the income table
    assert (result["Investment"] >= 0).all()


@pytest.mark.ut
def test_add_investment_expenses_are_negated(monthly_investments):
    expenses_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-29", "2020-03-31"]),
            "Home": [-100.0, -200.0],
        }
    )
    result = add_investment_expenses(expenses_wide, monthly_investments)
    # buys and order costs stored as negative outflows, matching the expense table
    assert result["Investment"].tolist() == [-175.0, -400.0]
    assert result["order_costs"].tolist() == [-2.0, -1.5]


@pytest.fixture
def monthly_rebalancing():
    # date stored as string (yyyy-MM-dd, month-end), as written to the CSV;
    # Feb is a net income month, Mar a net expense month
    return pd.DataFrame(
        {
            "date": ["2020-02-29", "2020-03-31"],
            "expense_rebalancing": [0.0, 400.0],
            "income_rebalancing": [50.0, 0.0],
        }
    )


@pytest.mark.ut
def test_add_rebalancing_income_folds_into_investment(monthly_rebalancing):
    incomes_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-29", "2020-03-31"]),
            "Salary": [1000.0, 1000.0],
            "Investment": [10.0, 20.0],
        }
    )
    result = add_rebalancing_income(incomes_wide, monthly_rebalancing)
    # net rebalancing income is added to Investment; no separate category
    assert "Rebalancing" not in result.columns
    assert result["Investment"].tolist() == [60.0, 20.0]  # 10+50, 20+0
    assert (result["Investment"] >= 0).all()


@pytest.mark.ut
def test_add_rebalancing_expenses_fold_into_investment(monthly_rebalancing):
    expenses_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-29", "2020-03-31"]),
            "Home": [-100.0, -200.0],
            "Investment": [-30.0, -40.0],
        }
    )
    result = add_rebalancing_expenses(expenses_wide, monthly_rebalancing)
    # net rebalancing expense is subtracted (negated) from Investment
    assert "Rebalancing" not in result.columns
    assert result["Investment"].tolist() == [-30.0, -440.0]  # -30-0, -40-400


@pytest.mark.ut
def test_add_rebalancing_missing_month_leaves_investment(monthly_rebalancing):
    expenses_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-31"]),
            "Home": [-100.0],
            "Investment": [-5.0],
        }
    )
    result = add_rebalancing_expenses(expenses_wide, monthly_rebalancing)
    # a month with no matching rebalancing row keeps its Investment unchanged
    assert result["Investment"].tolist() == [-5.0]


@pytest.fixture
def monthly_dividends():
    # long-format per-ETF dividends, date stored as string (month-end)
    return pd.DataFrame(
        {
            "date": ["2020-02-29", "2020-02-29", "2020-03-31"],
            "isin": ["A", "B", "A"],
            "name": ["ETF A", "ETF B", "ETF A"],
            "dividend": [10.0, 5.0, 8.0],
        }
    )


@pytest.mark.ut
def test_add_dividend_income_sums_per_month(monthly_dividends):
    incomes_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-29", "2020-03-31"]),
            "Salary": [1000.0, 1000.0],
        }
    )
    result = add_dividend_income(incomes_wide, monthly_dividends)
    # Feb sums both ETFs (10 + 5), Mar has a single ETF (8)
    assert result["Dividends"].tolist() == [15.0, 8.0]
    # income proceeds keep their positive sign, matching the income table
    assert (result["Dividends"] >= 0).all()


@pytest.mark.ut
def test_add_dividend_income_missing_month_filled_with_zero(monthly_dividends):
    incomes_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-31"]),
            "Salary": [1000.0],
        }
    )
    result = add_dividend_income(incomes_wide, monthly_dividends)
    assert result["Dividends"].tolist() == [0.0]


@pytest.fixture
def monthly_interest():
    # long-format per-category interest, date stored as string (month-end)
    return pd.DataFrame(
        {
            "date": ["2020-02-29", "2020-02-29", "2020-03-31"],
            "category": ["Tagesgeld", "Festgeld", "Tagesgeld"],
            "interest": [10.0, 5.0, 8.0],
        }
    )


@pytest.mark.ut
def test_add_interest_income_pivots_categories(monthly_interest):
    incomes_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-29", "2020-03-31"]),
            "Salary": [1000.0, 1000.0],
        }
    )
    result = add_interest_income(incomes_wide, monthly_interest)
    # each interest category becomes its own income column
    assert result["Tagesgeld"].tolist() == [10.0, 8.0]
    # March has no Festgeld interest -> filled with zero
    assert result["Festgeld"].tolist() == [5.0, 0.0]
    assert (result[["Tagesgeld", "Festgeld"]] >= 0).all().all()


@pytest.mark.ut
def test_add_interest_income_missing_month_filled_with_zero(monthly_interest):
    incomes_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-31"]),
            "Salary": [1000.0],
        }
    )
    result = add_interest_income(incomes_wide, monthly_interest)
    assert result["Tagesgeld"].tolist() == [0.0]
    assert result["Festgeld"].tolist() == [0.0]


@pytest.mark.ut
def test_add_interest_income_both_columns_present_when_one_kind_missing():
    # only Tagesgeld interest exists in the source, but both columns must appear
    interest = pd.DataFrame(
        {
            "date": ["2020-02-29"],
            "category": ["Tagesgeld"],
            "interest": [12.0],
        }
    )
    incomes_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-29"]),
            "Salary": [1000.0],
        }
    )
    result = add_interest_income(incomes_wide, interest)
    assert result["Tagesgeld"].tolist() == [12.0]
    assert result["Festgeld"].tolist() == [0.0]


@pytest.fixture
def investment_costs():
    # month-end aggregated investment costs, already expense-signed (negative),
    # as returned by load_investment_costs
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-29", "2020-03-31"]),
            "Investment Costs": [-12.5, -3.4],
        }
    )


@pytest.mark.ut
def test_add_investment_costs_merges_on_month(investment_costs):
    expenses_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-02-29", "2020-03-31"]),
            "Home": [-100.0, -200.0],
        }
    )
    result = add_investment_costs(expenses_wide, investment_costs)
    assert result["Investment Costs"].tolist() == [-12.5, -3.4]


@pytest.mark.ut
def test_add_investment_costs_missing_month_filled_with_zero(investment_costs):
    expenses_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-31"]),
            "Home": [-100.0],
        }
    )
    result = add_investment_costs(expenses_wide, investment_costs)
    assert result["Investment Costs"].tolist() == [0.0]


@pytest.mark.ut
def test_load_investment_costs_negates_and_aggregates(tmp_path):
    # all costs are stored as positive amounts (regardless of provider); a
    # negative stored cost is a credit that flips to a positive reduction.
    csv = (
        "Datum,Kosten,Anbieter,Kommentar,Name,ISIN,Note\n"
        "31.12.2020,3.52,comdirect,Wertpapierkosten 2020,ETF A,ISIN1,WKN a\n"
        "31.12.2020,-57.35,comdirect,Wertpapierkosten 2020,EUWAX,ISIN2,WKN b\n"
        "30.04.2022,35.4,traderepublic,TER 2021 alle ETFs,TER 2021,,\n"
        "20.01.2023,7.69,traderepublic,Steuerkorrektur,Gebuehren,,\n"
    )
    path = tmp_path / "comdirect_costs_combined.csv"
    path.write_text(csv, encoding="utf-8")

    result = load_investment_costs(path)
    result = result.set_index("date")["Investment Costs"]

    # Dec 2020: -(3.52 + -57.35) = -(-53.83) = 53.83 (net credit -> positive)
    assert result[pd.Timestamp("2020-12-31")] == pytest.approx(53.83)
    # costs negated into expenses and grouped to their month-end
    assert result[pd.Timestamp("2022-04-30")] == pytest.approx(-35.4)
    assert result[pd.Timestamp("2023-01-31")] == pytest.approx(-7.69)


@pytest.mark.ut
def test_missing_investment_month_filled_with_zero(monthly_investments):
    # a cashflow month without any matching investment row
    expenses_wide = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-31"]),
            "Home": [-100.0],
        }
    )
    result = add_investment_expenses(expenses_wide, monthly_investments)
    assert result["Investment"].tolist() == [0.0]
    assert result["order_costs"].tolist() == [0.0]
