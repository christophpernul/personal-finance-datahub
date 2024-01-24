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
    # Set source and target filepaths
    filepath_source = Path(DATAHUB_ROOT_FILEPATH) / "source" / "cashflow" / "toshl"
    filepath_target = Path(DATAHUB_ROOT_FILEPATH) / "target" / "cashflow"

    # Update complete cashflow data from Toshl
    stage = "A00"
    outpath = filepath_target / f"{stage}_toshl_cashflow.csv"

    a_00_cashflow = update_toshl_cashflow(
        source_root_path=filepath_source,
        raw_data_filepattern=TOSHL_SOURCE_FILEPATTERN,
    )
    save_data(
        data=a_00_cashflow,
        filepath=outpath,
    )
    logger.info(f"Complete cashflow written to {outpath}")

    # Clean cashflow data
    a_01_cashflow = cleaning_cashflow(a_00_cashflow)
    logger.info(f"Cashflow data cleaned!")

    # Load incomes from user input
    income_source_filepath = (
        Path(DATAHUB_ROOT_FILEPATH)
        / "source"
        / "cashflow"
        / "userinput"
        / "source_toshl_income.ods"
    )
    a_01_incomes = load_data(income_source_filepath, file_type="excel")
    logger.info(f"Income from user input loaded.")

    # Combine income data from toshl with user input incomes
    stage = "A10"

    a_10_incomes, a_10_expenses = split_cashflow_data(a_01_cashflow)
    a_11_incomes = combine_incomes(a_10_incomes, a_01_incomes)
    save_data(
        data=a_11_incomes,
        filepath=filepath_target / f"{stage}_incomes.csv",
    )
    save_data(
        data=a_10_expenses,
        filepath=filepath_target / f"{stage}_expenses.csv",
    )
    logger.info(f"Combined incomes and expenses saved in {filepath_target}")

    # Load toshl categorization and apply conversion to format required by dashboard
    stage = "B00"
    toshl_tag_categorization = get_config_file(TOSHL_CATEGORY_MAP)

    b_00_incomes = transform_cashflow_to_wide_format(
        a_11_incomes, toshl_tag_categorization["income"]
    )
    save_data(
        data=b_00_incomes,
        filepath=filepath_target / (f"{stage}_" + "incomes.csv"),
    )

    b_00_expenses = transform_cashflow_to_wide_format(
        a_10_expenses, toshl_tag_categorization["expenses"]
    )
    save_data(
        data=b_00_expenses,
        filepath=filepath_target / (f"{stage}_" + "expenses.csv"),
    )
    logger.info(
        f"Final cashflow expenses and incomes saved for usage in dashboard in {filepath_target}"
    )
    test = load_data(filepath=filepath_target / (f"{stage}_" + "expenses.csv"))
    logger.info("Cashflow preprocessing finished!")


if __name__ == "__main__":
    run_cashflow()
