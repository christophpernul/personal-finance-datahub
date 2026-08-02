"""Calculation of dashboard-ready target tables.

The target stage converts transform-stage data into the format consumed
directly by the dashboard, so no calculations are required there anymore.
"""

import logging
from functools import reduce

import pandas as pd

logger = logging.getLogger(__name__)


def transform_cashflow_to_wide_format(
    df: pd.DataFrame, tag_category_map: dict
) -> pd.DataFrame:
    """
    Remap tags of input data to custom categories, and change the format of the dataframe from longlist to wide format
    to easily do computations and plots of the cashflow data.
    Parameters
    ----------
    df: Contains cashflow data as longlist, [date, tag] are indices, amount is only column
    tag_category_map: Mapping of custom categories to a list of toshl tags. Needs to be category map for either income or expenses

    Returns
    -------
    Dataframe in wide format where each column is a category and date column is index
    """
    assert (
        isinstance(df.index, pd.core.indexes.multi.MultiIndex)
        and set(df.index.names) == {"date", "tag"}
        and list(df.columns) == ["amount"]
    ), "Dataframe is not grouped by month with a multi-index of [date, tag] and column amount!"
    ### Define custom categories for all tags of Toshl

    # Create all_category_lists, which is list of category values from custom category map
    # Reduce recursively flattens out all lists and results in one list of categories
    all_category_lists = [cat_list for cat_list in list(tag_category_map.values())]
    category_list = reduce(lambda x, y: x + y, all_category_lists)

    ### Create wide format from longlist, fill NaNs with zero and drop level 0 index "amount"
    pivot_init = df.unstack()
    pivot_init.fillna(0, inplace=True)
    pivot_init.columns = pivot_init.columns.droplevel()

    not_categorized = [tag for tag in pivot_init.columns if tag not in category_list]
    assert (
        len(not_categorized) == 0
    ), f"There are some tags, which are not yet categorized: {not_categorized}"

    # Calculate sum per categories and drop corresponding tags
    pivot = pivot_init.copy()
    for category, category_tags in tag_category_map.items():
        category_tags_in_data = list(
            set(category_tags).intersection(set(pivot.columns))
        )
        pivot[category] = pivot[category_tags_in_data].sum(axis=1)
        # Do not drop the newly created category column in case the custom category has the same name as one of the original ones
        category_columns_to_drop = list(
            set(category_tags_in_data).difference({category})
        )
        pivot.drop(columns=category_columns_to_drop, inplace=True)

    ### Keep only categories with non-zero total amount in result
    category_sum = pivot.sum().reset_index()
    nonzero_categories = list(category_sum[category_sum[0] != 0.0]["tag"])

    # Get date from index
    pivot = pivot[nonzero_categories].reset_index()

    return pivot


def _monthly_investments_by_date(monthly_investments: pd.DataFrame) -> pd.DataFrame:
    """Returns a copy of the monthly investments with a datetime `date` column
    (month-end) so it can be merged onto the wide cashflow tables."""
    investments = monthly_investments.copy()
    investments["date"] = pd.to_datetime(investments["date"])
    return investments


def add_investment_income(
    incomes_wide: pd.DataFrame, monthly_investments: pd.DataFrame
) -> pd.DataFrame:
    """Adds the monthly investment income (sell proceeds) from the ETF monthly
    investments to the wide cashflow income table as an `Investment` column,
    matched on month-end date. Incomes are positive, matching the sign of the
    sell proceeds.

    The column is added after the tag->category step, so it is intentionally
    not part of the toshl category mapping."""
    investments = _monthly_investments_by_date(monthly_investments)
    investments = investments.rename(columns={"income_investment": "Investment"})
    result = incomes_wide.merge(
        investments[["date", "Investment"]], on="date", how="left"
    )
    result["Investment"] = result["Investment"].fillna(0.0)

    # Combine the incomes tracked via Excel and toshl
    result["Investment"] = result["Investment"] + result["Investment Profit"]
    result.drop("Investment Profit", axis=1, inplace=True)
    return result


def add_rebalancing_income(
    incomes_wide: pd.DataFrame, monthly_rebalancing: pd.DataFrame
) -> pd.DataFrame:
    """Folds the monthly *net* rebalancing income into the existing `Investment`
    income column, matched on month-end date. A month is a rebalancing income
    only when the sell proceeds exceeded the buy spend and order fees; that net
    is added to `Investment` rather than tracked as its own category. Incomes
    stay positive, matching the sign of the income table.

    Must run after `add_investment_income`, which creates the `Investment`
    column."""
    rebalancing = monthly_rebalancing.copy()
    rebalancing["date"] = pd.to_datetime(rebalancing["date"])
    result = incomes_wide.merge(
        rebalancing[["date", "income_rebalancing"]], on="date", how="left"
    )
    result["income_rebalancing"] = result["income_rebalancing"].fillna(0.0)
    result["Investment"] = result["Investment"] + result["income_rebalancing"]
    result.drop(columns="income_rebalancing", inplace=True)
    return result


def add_rebalancing_expenses(
    expenses_wide: pd.DataFrame, monthly_rebalancing: pd.DataFrame
) -> pd.DataFrame:
    """Folds the monthly *net* rebalancing expense into the existing `Investment`
    expense column, matched on month-end date. A month is a rebalancing expense
    only when the buy spend and order fees exceeded the sell proceeds; that net
    is negated (to follow the expense table's convention of storing outflows as
    negative amounts) and added to `Investment` rather than tracked as its own
    category.

    Must run after `add_investment_expenses`, which creates the `Investment`
    column."""
    rebalancing = monthly_rebalancing.copy()
    rebalancing["date"] = pd.to_datetime(rebalancing["date"])
    result = expenses_wide.merge(
        rebalancing[["date", "expense_rebalancing"]], on="date", how="left"
    )
    result["expense_rebalancing"] = result["expense_rebalancing"].fillna(0.0)
    result["Investment"] = result["Investment"] - result["expense_rebalancing"]
    result.drop(columns="expense_rebalancing", inplace=True)
    return result


def add_dividend_income(
    incomes_wide: pd.DataFrame, monthly_dividends: pd.DataFrame
) -> pd.DataFrame:
    """Adds the monthly received ETF distributions from the per-ETF monthly
    dividends table to the wide cashflow income table as a `Dividends` column,
    matched on month-end date. The per-ETF dividends are summed to a single
    monthly total. Dividends are positive, matching the sign of the income table.

    The column is added after the tag->category step, so it is intentionally
    not part of the toshl category mapping."""
    dividends = monthly_dividends.copy()
    dividends["date"] = pd.to_datetime(dividends["date"])
    monthly_total = (
        dividends.groupby("date", as_index=False)["dividend"]
        .sum()
        .rename(columns={"dividend": "Dividends"})
    )
    result = incomes_wide.merge(monthly_total, on="date", how="left")
    result["Dividends"] = result["Dividends"].fillna(0.0)
    return result


def add_investment_expenses(
    expenses_wide: pd.DataFrame, monthly_investments: pd.DataFrame
) -> pd.DataFrame:
    """Adds the monthly investment expense (buys) as an `Investment` column and
    the `order_costs` from the ETF monthly investments to the wide cashflow
    expense table, matched on month-end date. Both are negated so they follow
    the expense table's convention of storing outflows as negative amounts.

    The columns are added after the tag->category step, so they are
    intentionally not part of the toshl category mapping."""
    investments = _monthly_investments_by_date(monthly_investments)
    investments["expense_investment"] = -investments["expense_investment"]
    investments["order_costs"] = -investments["order_costs"]
    investments = investments.rename(columns={"expense_investment": "Investment"})
    result = expenses_wide.merge(
        investments[["date", "Investment", "order_costs"]],
        on="date",
        how="left",
    )
    result[["Investment", "order_costs"]] = result[
        ["Investment", "order_costs"]
    ].fillna(0.0)
    return result
