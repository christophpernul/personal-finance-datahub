import logging
from pathlib import Path

from utils.file_io import get_config_file, save_data, load_data
from constants import (
    DATAHUB_ROOT_FILEPATH,
    TOSHL_CATEGORY_MAP,
    TOSHL_SOURCE_FILEPATTERN,
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
    # Set stage filepaths (flat folders, <stage>_<type>__<name>.csv convention)
    root = Path(DATAHUB_ROOT_FILEPATH)
    filepath_source = root / "source" / "cashflow" / "toshl"
    filepath_transform = root / "transform"
    filepath_target = root / "target"

    # Update complete cashflow data from Toshl
    outpath = filepath_transform / "transform_cashflow__toshl_cashflow.csv"

    combined_cashflow = update_toshl_cashflow(
        source_root_path=filepath_source,
        raw_data_filepattern=TOSHL_SOURCE_FILEPATTERN,
    )
    save_data(
        data=combined_cashflow,
        filepath=outpath,
    )
    logger.info(f"Complete cashflow written to {outpath}")

    # Clean cashflow data
    cleaned_cashflow = cleaning_cashflow(combined_cashflow)
    logger.info(f"Cashflow data cleaned!")

    # Combine income data from toshl with user input incomes
    incomes, expenses = split_cashflow_data(cleaned_cashflow)
    save_data(
        data=incomes,
        filepath=filepath_transform / "transform_cashflow__incomes.csv",
    )
    save_data(
        data=expenses,
        filepath=filepath_transform / "transform_cashflow__expenses.csv",
    )
    logger.info(f"Combined incomes and expenses saved in {filepath_transform}")

    # Load toshl categorization and apply conversion to format required by dashboard
    toshl_tag_categorization = get_config_file(TOSHL_CATEGORY_MAP)

    incomes_wide = transform_cashflow_to_wide_format(
        incomes, toshl_tag_categorization["income"]
    )
    save_data(
        data=incomes_wide,
        filepath=filepath_target / "target_cashflow__incomes.csv",
    )

    expenses_wide = transform_cashflow_to_wide_format(
        expenses, toshl_tag_categorization["expenses"]
    )
    save_data(
        data=expenses_wide,
        filepath=filepath_target / "target_cashflow__expenses.csv",
    )
    logger.info(
        f"Final cashflow expenses and incomes saved for usage in dashboard in {filepath_target}"
    )
    load_data(filepath=filepath_target / "target_cashflow__expenses.csv")
    logger.info("Cashflow preprocessing finished!")


if __name__ == "__main__":
    run_cashflow()
