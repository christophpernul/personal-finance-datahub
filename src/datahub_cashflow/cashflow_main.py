import logging

import pandas as pd

from utils.file_io import save_data
from constants import (
    TOSHL_SOURCE_FILEPATTERN,
    TOSHL_SOURCE_DIR,
    PATH_CASHFLOW_COMBINED,
    PATH_CASHFLOW_INCOMES,
    PATH_CASHFLOW_EXPENSES,
)
from datahub_cashflow.transform.transform_cashflow_data import (
    update_toshl_cashflow,
    split_cashflow_data,
    cleaning_cashflow,
)

logger = logging.getLogger(__name__)


def run_cashflow() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Runs the cashflow transform stage.

    Returns the monthly, multi-indexed ([date, tag]) income and expense tables
    so the target stage (`datahub_target.run_target`) can build the
    dashboard-ready tables from them.
    """
    # Update complete cashflow data from Toshl
    combined_cashflow = update_toshl_cashflow(
        source_root_path=TOSHL_SOURCE_DIR,
        raw_data_filepattern=TOSHL_SOURCE_FILEPATTERN,
    )
    save_data(
        data=combined_cashflow,
        filepath=PATH_CASHFLOW_COMBINED,
    )
    logger.info(f"Complete cashflow written to {PATH_CASHFLOW_COMBINED}")

    # Clean cashflow data
    cleaned_cashflow = cleaning_cashflow(combined_cashflow)
    logger.info(f"Cashflow data cleaned!")

    # Split into monthly incomes and expenses
    incomes, expenses = split_cashflow_data(cleaned_cashflow)
    save_data(
        data=incomes,
        filepath=PATH_CASHFLOW_INCOMES,
    )
    save_data(
        data=expenses,
        filepath=PATH_CASHFLOW_EXPENSES,
    )
    logger.info("Cashflow incomes and expenses saved in transform stage")

    return incomes, expenses


if __name__ == "__main__":
    run_cashflow()
