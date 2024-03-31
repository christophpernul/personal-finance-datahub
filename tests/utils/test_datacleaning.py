import pandas as pd
import pandas.testing as pd_testing
import pytest
from src.utils.datacleaning import clean, convert_columns_to_timestamp


@pytest.fixture
def test_dataframe():
    # Create a test DataFrame
    df = pd.DataFrame(
        {
            "A": ["  apple", "a banana  ", "  and a carrot  ", "date"],
            "B": ["  1", "2  ", " 3  ", "4"],
            "C": [5, 6, 7, 8],
        }
    )
    return df


@pytest.fixture
def expected_dataframe():
    # Create the expected DataFrame after cleaning
    df = pd.DataFrame(
        {
            "A": ["apple", "a banana", "and a carrot", "date"],
            "B": ["1", "2", "3", "4"],
            "C": [5, 6, 7, 8],
        }
    )
    return df


@pytest.mark.ut
def test_clean_function(test_dataframe, expected_dataframe):
    # Apply the clean function
    cleaned_df = clean(test_dataframe, ["A", "B"])

    # Check if the function has returned a DataFrame
    assert isinstance(cleaned_df, pd.DataFrame)

    # Check if the number of rows and columns remain the same
    pd.testing.assert_frame_equal(cleaned_df, expected_dataframe)


@pytest.mark.ut
def test_clean_function_non_string_column(test_dataframe):

    # Attempt to clean non-string column, it should raise a TypeError
    with pytest.raises(TypeError):
        clean(test_dataframe, ["C"])


@pytest.mark.ut
def test_convert_columns_to_timestamp():
    dates1 = ["2022-01-01", "2022-02-01", "2022-03-01"]
    dates2 = ["01-01-2022", "02-01-2022", "03-01-2022"]
    dates3 = ["01-Jan-2022", "02-Feb-2022", "03-Mar-2022"]
    df = pd.DataFrame({"date_col1": dates1, "date_col2": dates2, "date_col3": dates3})
    # Define column date formats
    column_formats = {
        "date_col1": "%Y-%m-%d",
        "date_col2": "%d-%m-%Y",
        "date_col3": "%d-%b-%Y",
    }

    # Apply the convert_columns_to_timestamp function
    converted_df = convert_columns_to_timestamp(df.copy(), column_formats)

    # Check if the function has returned a DataFrame
    assert isinstance(converted_df, pd.DataFrame)

    # Check if the data types of specified columns have been converted to timestamps
    for column, date_format in column_formats.items():
        assert pd.api.types.is_datetime64_any_dtype(converted_df[column])

    # Check if the values in the specified columns are converted correctly
    expected_date_col1 = pd.to_datetime(dates1, format=column_formats["date_col1"])
    expected_date_col2 = pd.to_datetime(dates2, format=column_formats["date_col2"])
    expected_date_col3 = pd.to_datetime(dates3, format=column_formats["date_col3"])
    df_expected = pd.DataFrame(
        {
            "date_col1": expected_date_col1,
            "date_col2": expected_date_col2,
            "date_col3": expected_date_col3,
        }
    )

    assert converted_df.sort_index(axis=1).equals(
        df_expected.sort_index(axis=1)
    ), "DataFrames are not equal"


@pytest.mark.ut
def test_convert_columns_to_timestamp_create_new():
    dates1 = ["2022-01-01", "2022-02-01", "2022-03-01"]
    dates2 = ["01-01-2022", "02-01-2022", "03-01-2022"]
    dates3 = ["01-Jan-2022", "02-Feb-2022", "03-Mar-2022"]
    df = pd.DataFrame({"date_col1": dates1, "date_col2": dates2, "date_col3": dates3})
    # Define column date formats
    column_formats = {
        "date_col1": "%Y-%m-%d",
        "date_col2": "%d-%m-%Y",
        "date_col3": "%d-%b-%Y",
    }

    # Apply the convert_columns_to_timestamp function
    converted_df = convert_columns_to_timestamp(df.copy(), column_formats, create=True)

    # Check if the function has returned a DataFrame
    assert isinstance(converted_df, pd.DataFrame)

    # Check if the data types of specified columns have been converted to timestamps
    for column, date_format in column_formats.items():
        assert pd.api.types.is_datetime64_any_dtype(converted_df[f"{column}_timestamp"])

    # Check if the values in the specified columns are converted correctly
    expected_date_col1 = pd.to_datetime(dates1, format=column_formats["date_col1"])
    expected_date_col2 = pd.to_datetime(dates2, format=column_formats["date_col2"])
    expected_date_col3 = pd.to_datetime(dates3, format=column_formats["date_col3"])
    df_expected = pd.DataFrame(
        {
            "date_col1": dates1,
            "date_col1_timestamp": expected_date_col1,
            "date_col2": dates2,
            "date_col2_timestamp": expected_date_col2,
            "date_col3": dates3,
            "date_col3_timestamp": expected_date_col3,
        }
    )

    pd_testing.assert_frame_equal(
        converted_df.sort_index(axis=1), df_expected.sort_index(axis=1)
    )
    assert converted_df.sort_index(axis=1).equals(
        df_expected.sort_index(axis=1)
    ), "DataFrames are not equal"
