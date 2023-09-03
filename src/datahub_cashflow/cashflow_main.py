from pathlib import Path
from typing import Dict
from src.datahub_library.file_handling_lib import get_datahub_config, save_data, load_data
from src.datahub_cashflow.transform.transform_cashflow_data import (update_toshl_cashflow, preprocess_cashflow,
                                                                    split_cashflow_data, cleaning_cashflow,
                                                                    combine_incomes)

# General datahub configuration
DATAHUB_CONFIG: Dict = get_datahub_config()
DATAHUB_CONFIG_META: Dict = DATAHUB_CONFIG["datahubMeta"]
DATAHUB_ROOT_FILEPATH: Path = DATAHUB_CONFIG_META["datahubRootFilepath"]
DATAHUB_SOURCE_FILEPATH: Path = Path(DATAHUB_ROOT_FILEPATH) / DATAHUB_CONFIG_META["sourceLayerName"]
DATAHUB_TRANSFORM_FILEPATH: Path = Path(DATAHUB_ROOT_FILEPATH) / DATAHUB_CONFIG_META["transformLayerName"]

# Specific cashflow datahub configuration
DATAHUB_CONFIG_CASHFLOW_META: Dict = DATAHUB_CONFIG["datahubCashflowMeta"]

if __name__ == "__main__":
    # Update cashflow data from Toshl
    filepath_toshl_cashflow = DATAHUB_TRANSFORM_FILEPATH / DATAHUB_CONFIG_CASHFLOW_META["filePath"]
    stage = "a_00"
    filepath = filepath_toshl_cashflow / (f"{stage}_" + DATAHUB_CONFIG_CASHFLOW_META["transformFileName"])
    a_00_cashflow = update_toshl_cashflow(datahub_source_root_path=DATAHUB_SOURCE_FILEPATH,
                                        datahub_cashflow_config=DATAHUB_CONFIG_CASHFLOW_META,
                                        )
    save_data(data=a_00_cashflow,
              filepath=filepath,
              )
    a_01_cashflow = cleaning_cashflow(a_00_cashflow)

    income_source_filepath = DATAHUB_SOURCE_FILEPATH / "cashflow" / "userinput" / "source_toshl_income.ods"
    a_01_incomes = load_data(income_source_filepath, file_type="excel")

    # TODO: Is this necessary?
    # TODO: Do not hardcode filenames here!
    stage = "a_10"
    filepath_incomes = filepath_toshl_cashflow / (f"{stage}_" + "incomes.csv")
    filepath_expenses = filepath_toshl_cashflow / (f"{stage}_" + "expenses.csv")
    a_10_incomes, a_10_expenses = split_cashflow_data(a_01_cashflow)
    a_11_incomes = combine_incomes(a_10_incomes, a_01_incomes)
    save_data(data=a_11_incomes,
              filepath=filepath_incomes,
              )
    save_data(data=a_10_expenses,
              filepath=filepath_expenses,
              )

    # TODO: Simplify preprocessing
    stage = "a_20"
    filepath = filepath_toshl_cashflow / (f"{stage}_" + "incomes.csv")
    a_20_caution, a_20_incomes = preprocess_cashflow(a_11_incomes)
    save_data(data=a_20_incomes,
              filepath=filepath,
              )
