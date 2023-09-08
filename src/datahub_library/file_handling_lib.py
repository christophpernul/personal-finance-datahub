import json
from pathlib import Path
import pandas as pd
from warnings import warn
from typing import Dict


def get_datahub_config(config_file_name: str = "datahub_config.json") -> Dict:
    """
    Loads the config file used to provide configuration for the datahub.

    Parameters
    ----------
    config_file_name: Name of config file and file type .json

    Returns
    -------

    """
    for path in Path.cwd().parents:
        config_path: Path = path / config_file_name
        if config_path.is_file():
            with config_path.open("r") as cfile:
                config = json.load(cfile)
            return config
    else:
        raise FileNotFoundError(f"Config file {config_file_name} not found!")


def load_data(
    filepath: Path, used_library: str = "pandas", file_type: str = "csv"
) -> pd.DataFrame | None:
    """
    Loads data from given filepath and given file_extension
    Parameters
    ----------
    filepath: Path including filename and file extension
    used_library: Specifies the python library to load data

    Returns
    -------

    """
    implemented_libraries = "pandas"
    # TODO: Change filetype to excel instead of odf, add check that nothing else gets handed over
    allowed_file_types = ("csv", "odf")
    # if file_type == "csv":
    #     load_func = pd.read_csv
    # elif file_type == "odf":
    #     load_func = pd.read_excel
    # TODO: Add polars
    if used_library == "pandas" and file_type == "csv":
        return pd.read_csv(filepath_or_buffer=filepath)
    elif used_library == "pandas" and file_type == "excel":
        return pd.read_excel(filepath, engine="odf")
    elif used_library != "pandas":
        warn(
            f"Provided library is not supported yet! Use one of the following: {''.join(implemented_libraries)}"
        )


def save_data(data: pd.DataFrame, filepath: Path, used_library: str = "pandas") -> None:
    """
    Saves data to filepath using the used_library as engine.
    Parameters
    ----------
    data: python object containing data (has to match the used library)
    filepath: file path where the data should be stored
    used_library: specifies the python library used to store the data

    Returns
    -------

    """
    implemented_libraries = "pandas"
    # TODO: Add polars
    if used_library == "pandas":
        return data.reset_index().to_csv(path_or_buf=filepath, index=False)
    if used_library != "pandas":
        warn(
            f"Provided library is not supported yet! Use one of the following: {''.join(implemented_libraries)}"
        )
