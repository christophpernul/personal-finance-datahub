import pandas as pd
import pytest

from src.datahub_target.transform.calculate_target_tables import (
    add_investment_income,
    add_dividend_income,
    add_investment_expenses,
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
