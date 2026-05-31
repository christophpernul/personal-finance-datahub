"""Provides reusable functionality to clean data and convert datatypes."""

import pandas as pd


def strip_vals(data: pd.DataFrame, strip_columns: list) -> pd.DataFrame:
    """
    Clean the values of specified columns in a DataFrame by stripping leading and trailing whitespaces.

    Parameters
    ----------
    data : pd.DataFrame
        The input DataFrame.

    strip_columns : list
        List of column names to be cleaned.

    Returns
    -------
    pd.DataFrame
        Cleaned DataFrame.

    Raises
    ------
    TypeError
        If any of the specified columns are not of type string.
    """
    # Check if all specified columns are of type string
    for column in strip_columns:
        if data[column].dtype != "object":
            raise TypeError(f"Column '{column}' is not of type string.")

    # Strip leading and trailing whitespaces from specified columns
    for column in strip_columns:
        data[column] = data[column].str.strip()

    return data


def convert_columns_to_datatype(
    data: pd.DataFrame, column_datatypes: dict
) -> pd.DataFrame:
    """
    Convert specified columns in a DataFrame to the provided data types.

    Parameters
    ----------
    data : pd.DataFrame
        The input DataFrame.

    column_datatypes : dict
        Dictionary where keys are column names and values are desired data types.

    Returns
    -------
    pd.DataFrame
        DataFrame with specified columns converted to the provided data types.
    """
    for column, datatype in column_datatypes.items():
        if column in data.columns:
            data[column] = data[column].astype(datatype)
    return data


def convert_columns_to_timestamp(
    data: pd.DataFrame, column_formats: dict, create: bool = False
) -> pd.DataFrame:
    """
    Convert specified columns in a DataFrame to timestamps using the provided date formats.

    Parameters
    ----------
    data : pd.DataFrame
        The input DataFrame.

    column_formats : dict
        Dictionary where keys are column names and values are date formats to parse the timestamps.

    create : bool, optional
        If True, create new timestamp columns, by default False.

    Returns
    -------
    pd.DataFrame
        DataFrame with specified columns converted to timestamps.
    """
    for column, date_format in column_formats.items():
        if column in data.columns:
            column_timestamp = pd.to_datetime(data[column], format=date_format)
            if create:
                data[f"{column}_timestamp"] = column_timestamp
            else:
                data[column] = column_timestamp
    return data
