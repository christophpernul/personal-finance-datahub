import logging

from utils.file_io import get_config_file, save_data, load_data
from constants import (
    TOSHL_CATEGORY_MAP,
    TOSHL_SOURCE_FILEPATTERN,
    TOSHL_SOURCE_DIR,
    PATH_CASHFLOW_COMBINED,
    PATH_CASHFLOW_INCOMES,
    PATH_CASHFLOW_EXPENSES,
    PATH_CASHFLOW_INCOMES_WIDE,
    PATH_CASHFLOW_EXPENSES_WIDE,
)
from datahub_cashflow.transform.transform_cashflow_data import (
    update_toshl_cashflow,
    transform_cashflow_to_wide_format,
    split_cashflow_data,
    cleaning_cashflow,
    combine_incomes,
)

logger = logging.getLogger(__name__)


def run_cashflow():
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

    # Combine income data from toshl with user input incomes
    incomes, expenses = split_cashflow_data(cleaned_cashflow)
    save_data(
        data=incomes,
        filepath=PATH_CASHFLOW_INCOMES,
    )
    save_data(
        data=expenses,
        filepath=PATH_CASHFLOW_EXPENSES,
    )
    logger.info("Combined incomes and expenses saved in transform stage")

    # Load toshl categorization and apply conversion to format required by dashboard
    toshl_tag_categorization = get_config_file(TOSHL_CATEGORY_MAP)

    incomes_wide = transform_cashflow_to_wide_format(
        incomes, toshl_tag_categorization["income"]
    )
    save_data(
        data=incomes_wide,
        filepath=PATH_CASHFLOW_INCOMES_WIDE,
    )

    expenses_wide = transform_cashflow_to_wide_format(
        expenses, toshl_tag_categorization["expenses"]
    )
    save_data(
        data=expenses_wide,
        filepath=PATH_CASHFLOW_EXPENSES_WIDE,
    )
    logger.info(
        f"Final cashflow expenses and incomes saved for usage in dashboard in {PATH_CASHFLOW_EXPENSES_WIDE.parent}"
    )
    load_data(filepath=PATH_CASHFLOW_EXPENSES_WIDE)
    logger.info("Cashflow preprocessing finished!")


if __name__ == "__main__":
    run_cashflow()
