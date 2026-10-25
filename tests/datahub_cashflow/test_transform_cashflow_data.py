import pandas as pd
import pytest

from src.datahub_cashflow.transform.transform_cashflow_data import (
    cleaning_cashflow,
    split_cashflow_data,
    split_off_replaced_bookings,
)


@pytest.fixture
def toshl_export():
    # raw toshl export: US dates, 1000s separators, Description may be empty
    return pd.DataFrame(
        {
            "Date": ["9/1/26", "9/2/26", "9/3/26"],
            "Account": ["Cash", "Cash", "Cash"],
            "Category": ["Home & Utilities", "Food & Drinks", "Salary"],
            "Tags": ["rent", "groceries", "Salary"],
            "Expense amount": ["1,285.00", "12.50", "0"],
            "Income amount": ["0", "0", "3,000.00"],
            "Currency": ["EUR", "EUR", "EUR"],
            "In main currency": ["1,285.00", "12.50", "3,000.00"],
            "Main currency": ["EUR", "EUR", "EUR"],
            "Description": ["Warmmiete ", None, "September"],
        }
    )


@pytest.mark.ut
def test_cleaning_cashflow_keeps_description(toshl_export):
    result = cleaning_cashflow(toshl_export)
    assert list(result.columns) == ["date", "tag", "amount", "description"]
    # stripped, and an empty string where no description was entered
    assert result["description"].tolist() == ["Warmmiete", "", "September"]
    assert result["amount"].tolist() == [-1285.0, -12.5, 3000.0]


@pytest.mark.ut
def test_split_cashflow_data_carries_description(toshl_export):
    incomes, expenses = split_cashflow_data(cleaning_cashflow(toshl_export))
    assert incomes["description"].tolist() == ["September"]
    assert expenses["description"].tolist() == ["Warmmiete", ""]


@pytest.mark.ut
def test_split_off_replaced_bookings_sets_aside_categories_and_exclusions(
    toshl_export,
):
    toshl_export.loc[2, "Category"] = "Crypto"
    excluded = pd.DataFrame(
        {
            "date": ["2026-09-02"],
            "tag": ["groceries"],
            "amount": [12.5],
            "description": [""],
            "comment": ["contained in the crypto exchange data"],
        }
    )
    kept, replaced = split_off_replaced_bookings(toshl_export, ["Crypto"], excluded)
    assert kept["Tags"].tolist() == ["rent"]
    assert replaced["Tags"].tolist() == ["groceries", "Salary"]
    assert replaced["reason"].tolist() == [
        "contained in the crypto exchange data",
        "toshl category booked from another datahub source",
    ]


@pytest.mark.ut
def test_split_off_replaced_bookings_warns_about_unmatched_exclusions(
    toshl_export, caplog
):
    excluded = pd.DataFrame(
        {
            "date": ["2026-09-02"],
            "tag": ["groceries"],
            "amount": [99.0],
            "description": [""],
            "comment": ["typo in the amount"],
        }
    )
    kept, replaced = split_off_replaced_bookings(toshl_export, [], excluded)
    assert len(kept) == 3 and replaced.empty
    assert "matches 0 bookings" in caplog.text
