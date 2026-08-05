"""Calculation of dashboard-ready target tables.

The target stage converts transform-stage data into the format consumed
directly by the dashboard, so no calculations are required there anymore.
"""

import logging
from functools import reduce
from pathlib import Path

import pandas as pd

from utils.file_io import load_data

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


def add_interest_income(
    incomes_wide: pd.DataFrame, monthly_interest: pd.DataFrame
) -> pd.DataFrame:
    """Adds the monthly received interest to the wide cashflow income table as a
    `Tagesgeld` and a `Festgeld` column, matched on month-end date. The
    per-category monthly interest table is pivoted so each interest category
    becomes its own income column. Interest amounts are positive, matching the
    sign of the income table. Both columns are always present (filled with 0.0 in
    months without interest of that kind).

    The columns are added after the tag->category step, so they are intentionally
    not part of the toshl category mapping."""
    categories = ["Tagesgeld", "Festgeld"]
    interest = monthly_interest.copy()
    interest["date"] = pd.to_datetime(interest["date"])
    wide = (
        interest.pivot_table(
            index="date", columns="category", values="interest", aggfunc="sum"
        )
        .reset_index()
        .rename_axis(columns=None)
    )
    for category in categories:
        if category not in wide.columns:
            wide[category] = 0.0

    result = incomes_wide.merge(wide[["date", *categories]], on="date", how="left")
    result[categories] = result[categories].fillna(0.0)
    return result


def load_investment_costs(filepath: Path) -> pd.DataFrame:
    """Loads the combined investment-cost report (comdirect cost reports +
    Trade Republic fees) and aggregates it into a monthly (month-end)
    ``Investment Costs`` expense total.

    The source file stores each cost in the ``Kosten`` column as a *positive*
    EUR amount (a cost you paid), regardless of provider. The target expense
    tables store outflows as negative amounts, so every cost is negated. A
    *negative* cost in the file (e.g. the 2020 EUWAX-Gold credit) therefore
    becomes a positive cost reduction.

    Returns a dataframe with a month-end ``date`` column and a single
    ``Investment Costs`` column, ready to be merged onto the wide expense table.
    """
    costs = load_data(filepath, sep=",", decimal=".")
    costs["date"] = pd.to_datetime(costs["Datum"], format="%d.%m.%Y")

    # Normalise to the expense sign convention: costs are stored positive, so
    # negate them into negative outflows (a stored credit flips to a reduction).
    costs["amount"] = -costs["Kosten"]

    monthly = (
        costs.groupby(pd.Grouper(key="date", freq="ME"))["amount"]
        .sum()
        .reset_index()
        .rename(columns={"amount": "Investment Costs"})
    )
    return monthly


def add_investment_costs(
    expenses_wide: pd.DataFrame, investment_costs: pd.DataFrame
) -> pd.DataFrame:
    """Adds the monthly investment costs (comdirect cost reports + Trade Republic
    fees) to the wide expense table as an ``Investment Costs`` column, matched on
    month-end date. The amounts are already expense-signed (negative outflows) by
    ``load_investment_costs``, so they are merged verbatim. Cashflow months
    without any investment cost are filled with 0.0.

    The column is added after the tag->category step, so it is intentionally not
    part of the toshl category mapping."""
    result = expenses_wide.merge(investment_costs, on="date", how="left")

    # A cost dated in a month that is absent from the cashflow table would be
    # silently dropped by the left merge; warn so it is not lost unnoticed.
    unmatched = set(investment_costs["date"]).difference(set(expenses_wide["date"]))
    if unmatched:
        logger.warning(
            "Investment cost months not present in the cashflow expense table "
            "and therefore dropped: %s",
            sorted(str(d.date()) for d in unmatched),
        )

    result["Investment Costs"] = result["Investment Costs"].fillna(0.0)
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


def add_riester_expenses(
    expenses_wide: pd.DataFrame, monthly_riester: pd.DataFrame
) -> pd.DataFrame:
    """Adds the monthly Riester pension contributions as a `Riester` column to the
    wide cashflow expense table, matched on month-end date. The contributions are
    negated so they follow the expense table's convention of storing outflows as
    negative amounts. Months without a contribution are filled with 0.0.

    The column is added after the tag->category step, so it is intentionally not
    part of the toshl category mapping."""
    riester = monthly_riester.copy()
    riester["date"] = pd.to_datetime(riester["date"])
    riester["Riester"] = -riester["riester"]
    result = expenses_wide.merge(riester[["date", "Riester"]], on="date", how="left")
    result["Riester"] = result["Riester"].fillna(0.0)
    return result
