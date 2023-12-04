from pathlib import Path
from typing import Dict
from datahub_library.file_handling_lib import get_config_file, save_data, load_data
from datahub_cashflow.transform.transform_cashflow_data import (
    update_toshl_cashflow,
    transform_cashflow_to_wide_format,
    split_cashflow_data,
    cleaning_cashflow,
    combine_incomes,
)

# General datahub configuration
DATAHUB_CONFIG: Dict = get_config_file()
DATAHUB_CONFIG_META: Dict = DATAHUB_CONFIG["datahubMeta"]
DATAHUB_ROOT_FILEPATH: Path = DATAHUB_CONFIG_META["datahubRootFilepath"]
DATAHUB_SOURCE_FILEPATH: Path = (
    Path(DATAHUB_ROOT_FILEPATH) / DATAHUB_CONFIG_META["sourceLayerName"]
)
DATAHUB_TRANSFORM_FILEPATH: Path = (
    Path(DATAHUB_ROOT_FILEPATH) / DATAHUB_CONFIG_META["transformLayerName"]
)
DATAHUB_APPLICATION_FILEPATH: Path = (
    Path(DATAHUB_ROOT_FILEPATH) / DATAHUB_CONFIG_META["applicationLayerName"]
)

# Specific cashflow datahub configuration
DATAHUB_CONFIG_CASHFLOW_META: Dict = DATAHUB_CONFIG["datahubCashflowMeta"]

if __name__ == "__main__":
    # Update cashflow data from Toshl
    filepath_toshl_cashflow_source = (
        DATAHUB_SOURCE_FILEPATH
        / DATAHUB_CONFIG_CASHFLOW_META["relativePath"]
        / DATAHUB_CONFIG_CASHFLOW_META["toshl"]["relativePath"]
    )
    toshl_raw_data_filepattern = DATAHUB_CONFIG_CASHFLOW_META["toshl"][
        "sourceFilePattern"
    ]

    filepath_cashflow_transform_output = (
        DATAHUB_TRANSFORM_FILEPATH / DATAHUB_CONFIG_CASHFLOW_META["relativePath"]
    )

    stage = "a_00"
    outpath = filepath_cashflow_transform_output / f"{stage}_full_cashflow.csv"
    a_00_cashflow = update_toshl_cashflow(
        source_root_path=filepath_toshl_cashflow_source,
        raw_data_filepattern=toshl_raw_data_filepattern,
    )
    save_data(
        data=a_00_cashflow,
        filepath=outpath,
    )
    a_01_cashflow = cleaning_cashflow(a_00_cashflow)

    income_source_filepath = (
        DATAHUB_SOURCE_FILEPATH / "cashflow" / "userinput" / "source_toshl_income.ods"
    )
    a_01_incomes = load_data(income_source_filepath, file_type="excel")

    # Combine income data from toshl with manual incomes
    stage = "a_10"
    outpath_incomes = filepath_cashflow_transform_output / f"{stage}_incomes.csv"
    outpath_expenses = filepath_cashflow_transform_output / f"{stage}_expenses.csv"

    a_10_incomes, a_10_expenses = split_cashflow_data(a_01_cashflow)
    a_11_incomes = combine_incomes(a_10_incomes, a_01_incomes)
    save_data(
        data=a_11_incomes,
        filepath=outpath_incomes,
    )
    save_data(
        data=a_10_expenses,
        filepath=outpath_expenses,
    )

    # TODO: Simplify preprocessing
    toshl_tag_categorization = get_config_file(
        DATAHUB_CONFIG_CASHFLOW_META["toshl"]["configFileName"]
    )
    filepath_cashflow_application_output = (
        DATAHUB_APPLICATION_FILEPATH / DATAHUB_CONFIG_CASHFLOW_META["relativePath"]
    )
    stage = "b_00"
    outpath = filepath_cashflow_application_output / (f"{stage}_" + "incomes.csv")
    b_00_incomes = transform_cashflow_to_wide_format(
        a_11_incomes, toshl_tag_categorization["income"]
    )
    save_data(
        data=b_00_incomes,
        filepath=outpath,
    )

    outpath = filepath_cashflow_application_output / (f"{stage}_" + "expenses.csv")
    b_00_expenses = transform_cashflow_to_wide_format(
        a_10_expenses, toshl_tag_categorization["expenses"]
    )
    save_data(
        data=b_00_expenses,
        filepath=outpath,
    )

    test = load_data(filepath=outpath)
