"""Calculates all target (dashboard-ready) tables from transform-stage data.

The target stage is the single place that produces the tables consumed by
the dashboard, so the dashboard itself needs no further calculations.
"""

import logging

import pandas as pd

from utils.file_io import get_config_file, save_data, load_data
from constants import (
    TOSHL_CATEGORY_MAP,
    PATH_ETF_MONTHLY_INVESTMENTS,
    PATH_CASHFLOW_INCOMES_WIDE,
    PATH_CASHFLOW_EXPENSES_WIDE,
)
from datahub_target.transform.calculate_target_tables import (
    transform_cashflow_to_wide_format,
    add_investment_income,
    add_dividend_income,
    add_investment_expenses,
    add_rebalancing_income,
    add_rebalancing_expenses,
)

logger = logging.getLogger(__name__)


def run_target(
    cashflow_incomes: pd.DataFrame,
    cashflow_expenses: pd.DataFrame,
    monthly_investments: pd.DataFrame,
    monthly_dividends: pd.DataFrame,
    monthly_rebalancing: pd.DataFrame,
) -> None:
    """Calculates and stores all dashboard-ready target tables.

    Parameters
    ----------
    cashflow_incomes / cashflow_expenses: transform-stage cashflow tables
        (monthly, multi-indexed by [date, tag]) as returned by
        `datahub_cashflow.run_cashflow`.
    monthly_investments / monthly_dividends / monthly_rebalancing: transform-stage
        ETF tables as returned by `datahub_stocks.run_stocks`.
    """
    calculate_cashflow_target_tables(
        cashflow_incomes,
        cashflow_expenses,
        monthly_investments,
        monthly_dividends,
        monthly_rebalancing,
    )


def calculate_cashflow_target_tables(
    incomes: pd.DataFrame,
    expenses: pd.DataFrame,
    monthly_investments: pd.DataFrame,
    monthly_dividends: pd.DataFrame,
    monthly_rebalancing: pd.DataFrame,
) -> None:
    """Builds the wide-format cashflow tables, enriches them with the ETF
    monthly investment, dividend and rebalancing columns, and stores them as
    target tables."""
    toshl_tag_categorization = get_config_file(TOSHL_CATEGORY_MAP)

    incomes_wide = transform_cashflow_to_wide_format(
        incomes, toshl_tag_categorization["income"]
    )
    expenses_wide = transform_cashflow_to_wide_format(
        expenses, toshl_tag_categorization["expenses"]
    )

    # Enrich the cashflow tables with the ETF monthly investment columns before
    # storing them, so the dashboard finds everything in a single table. Received
    # ETF distributions are added as an additional income column. The rebalancing
    # net is folded into the Investment column (not a separate category), so it
    # must run after the Investment column has been created.
    incomes_wide = add_investment_income(incomes_wide, monthly_investments)
    incomes_wide = add_dividend_income(incomes_wide, monthly_dividends)
    incomes_wide = add_rebalancing_income(incomes_wide, monthly_rebalancing)
    expenses_wide = add_investment_expenses(expenses_wide, monthly_investments)
    expenses_wide = add_rebalancing_expenses(expenses_wide, monthly_rebalancing)

    save_data(data=incomes_wide, filepath=PATH_CASHFLOW_INCOMES_WIDE)
    save_data(data=expenses_wide, filepath=PATH_CASHFLOW_EXPENSES_WIDE)
    logger.info(
        f"Cashflow target tables (incomes, expenses) with investment columns "
        f"saved for the dashboard in {PATH_CASHFLOW_EXPENSES_WIDE.parent}"
    )
