"""Calculates all target (dashboard-ready) tables from transform-stage data.

The target stage is the single place that produces the tables consumed by
the dashboard, so the dashboard itself needs no further calculations.
"""

import logging

import pandas as pd

from utils.file_io import get_config_file, save_data
from constants import (
    TOSHL_CATEGORY_MAP,
    PATH_INVESTMENT_COSTS,
    PATH_CASHFLOW_INCOMES_TARGET,
    PATH_CASHFLOW_EXPENSES_TARGET,
    PATH_CASHFLOW_INCOMES_WIDE,
    PATH_CASHFLOW_EXPENSES_WIDE,
)
from datahub_target.transform.calculate_target_tables import (
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

logger = logging.getLogger(__name__)


def run_target(
    cashflow_incomes: pd.DataFrame,
    cashflow_expenses: pd.DataFrame,
    investment_trades: pd.DataFrame,
    dividends: pd.DataFrame,
    monthly_rebalancing: pd.DataFrame,
    interest: pd.DataFrame,
    riester: pd.DataFrame,
) -> None:
    """Calculates and stores all dashboard-ready target tables.

    Parameters
    ----------
    cashflow_incomes / cashflow_expenses: transform-stage cashflow tables
        holding single toshl transactions ([date, tag, amount]) as returned by
        `datahub_cashflow.run_cashflow`.
    investment_trades / dividends / interest / riester: transform-stage ETF,
        stock, interest and Riester tables holding single transactions, as
        returned by `datahub_stocks.run_stocks`.
    monthly_rebalancing: the monthly *net* rebalancing cashflow, which has no
        transaction-level equivalent (see `build_rebalancing_transactions`).
    """
    calculate_cashflow_target_tables(
        cashflow_incomes,
        cashflow_expenses,
        investment_trades,
        dividends,
        monthly_rebalancing,
        interest,
        riester,
    )


def calculate_cashflow_target_tables(
    incomes: pd.DataFrame,
    expenses: pd.DataFrame,
    investment_trades: pd.DataFrame,
    dividends: pd.DataFrame,
    monthly_rebalancing: pd.DataFrame,
    interest: pd.DataFrame,
    riester: pd.DataFrame,
) -> None:
    """Builds the transaction-level cashflow tables from all sources, stores
    them, and stores the monthly wide-format aggregation of the very same
    transactions alongside them."""
    toshl_tag_categorization = get_config_file(TOSHL_CATEGORY_MAP)

    # Which category map applies depends on the side a transaction landed on:
    # `Kaution` is a deposit paid (an expense) as well as one returned (an income).
    incomes_transactions = map_tags_to_categories(
        incomes, toshl_tag_categorization["income"]
    )
    expenses_transactions = map_tags_to_categories(
        expenses, toshl_tag_categorization["expenses"]
    )

    # Investment profits tracked in toshl share the `Investment` category with
    # the ETF trades below instead of staying a category of their own.
    incomes_transactions["category"] = incomes_transactions["category"].replace(
        {"Investment Profit": "Investment"}
    )

    # Everything that is not tracked in toshl is added as its own transactions,
    # so the dashboard finds every booking in a single table. Received ETF
    # distributions, stock dividends and cash-deposit interest (Tagesgeld /
    # Festgeld) are incomes, Riester contributions and investment costs are
    # expenses, and the trades are either, depending on buy or sell.
    additional_transactions = pd.concat(
        [
            build_investment_transactions(investment_trades),
            build_rebalancing_transactions(monthly_rebalancing),
            build_dividend_transactions(dividends),
            build_interest_transactions(interest),
            build_riester_transactions(riester),
            load_investment_cost_transactions(PATH_INVESTMENT_COSTS),
        ],
        ignore_index=True,
    )

    # As for the toshl transactions, the sign of the amount alone decides which
    # side a transaction belongs to (zero amounts were already dropped).
    incomes_transactions = _combine_transactions(
        incomes_transactions,
        additional_transactions[additional_transactions["amount"] > 0.0],
    )
    expenses_transactions = _combine_transactions(
        expenses_transactions,
        additional_transactions[additional_transactions["amount"] < 0.0],
    )

    save_data(data=incomes_transactions, filepath=PATH_CASHFLOW_INCOMES_TARGET)
    save_data(data=expenses_transactions, filepath=PATH_CASHFLOW_EXPENSES_TARGET)
    logger.info(
        f"Cashflow target tables (incomes, expenses) with "
        f"{len(incomes_transactions)} income and {len(expenses_transactions)} "
        f"expense transactions saved for the dashboard in "
        f"{PATH_CASHFLOW_EXPENSES_TARGET.parent}"
    )

    # The monthly cashflow is aggregated from the very same transactions, so
    # both granularities always agree.
    incomes_wide = transform_cashflow_to_wide_format(
        aggregate_transactions_monthly(incomes_transactions)
    )
    expenses_wide = transform_cashflow_to_wide_format(
        aggregate_transactions_monthly(expenses_transactions)
    )

    save_data(data=incomes_wide, filepath=PATH_CASHFLOW_INCOMES_WIDE)
    save_data(data=expenses_wide, filepath=PATH_CASHFLOW_EXPENSES_WIDE)
    logger.info(
        f"Monthly cashflow target tables (incomes, expenses) saved for the "
        f"dashboard in {PATH_CASHFLOW_EXPENSES_WIDE.parent}"
    )


def _combine_transactions(
    toshl_transactions: pd.DataFrame, additional_transactions: pd.DataFrame
) -> pd.DataFrame:
    """Appends the non-toshl transactions to the toshl ones and sorts the result
    chronologically."""
    return (
        pd.concat([toshl_transactions, additional_transactions], ignore_index=True)
        .sort_values("date", kind="stable")
        .reset_index(drop=True)
    )
