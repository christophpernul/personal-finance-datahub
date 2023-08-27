from pathlib import Path
from typing import Dict
from src.datahub_library.file_handling_lib import get_datahub_config, save_data
from src.datahub_cashflow.transform.transform_cashflow_data import update_toshl_cashflow

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
    filepath_toshl_cashflow = DATAHUB_TRANSFORM_FILEPATH / DATAHUB_CONFIG_CASHFLOW_META["filePath"] / \
                              DATAHUB_CONFIG_CASHFLOW_META["transformFileName"]
    df_cashflow = update_toshl_cashflow(datahub_source_root_path=DATAHUB_SOURCE_FILEPATH,
                                        datahub_cashflow_config=DATAHUB_CONFIG_CASHFLOW_META,
                                        )
    save_data(data=df_cashflow,
              filepath=filepath_toshl_cashflow,
              )
