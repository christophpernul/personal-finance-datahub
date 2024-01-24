import pandas as pd
import logging
from pathlib import Path
from functools import reduce

from utils.file_io import load_data

logger = logging.getLogger(__name__)


def update_toshl_cashflow(
    source_root_path: Path, raw_data_filepattern: str
) -> pd.DataFrame:
    """
    Iterates over all files in source_root_path and filters for files with raw_data_filepattern.
    Parameters
    ----------
    source_root_path: Path to a folder containing cashflow data from Toshl
    raw_data_filepattern: filepattern for the files

    Returns
    -------

    """
    raw_data_files: [Path] = sorted(source_root_path.glob(raw_data_filepattern))
    for cnt, raw_file_path in enumerate(raw_data_files):
        df: pd.DataFrame = load_data(raw_file_path)
        assert (
            df.drop("Description", axis=1).isna().sum().sum() == 0
        ), f"There are NaN values in Toshl data!"
        if cnt == 0:
            df_cashflow: pd.DataFrame = df.copy()
        else:
            df_cashflow = pd.concat([df_cashflow, df], ignore_index=True)
        logger.info(f"File {raw_file_path.name} contains {df.count().iloc[0]} rows.")
    return df_cashflow


def cleaning_cashflow(df: pd.DataFrame) -> pd.DataFrame:
    """
    Data cleaning and preprocessing of cashflow data:
    1. Rename columns to snakecase names
    2. Drop 1000s separators (,) and convert datatypes
    3. Create a unique column containing the amount spent or received
    4. Map all rows containing Urlaub to a vacation tag
    Parameters
    ----------
    df: containing all cashflow data

    Returns
    -------
    cleaned cashflow data
    """
    column_name_mapping = {
        "Date": "date",
        "Account": "account",
        "Category": "category",
        "Tags": "tag",
        "Expense amount": "expense_amount",
        "Income amount": "income_amount",
        "Currency": "currency",
        "In main currency": "amount_main_currency",
        "Main currency": "main_currency",
        "Description": "description",
    }
    relevant_columns = [
        "date",
        "category",
        "tag",
        "expense_amount",
        "income_amount",
        "amount_main_currency",
    ]
    output_columns = [
        "date",
        "tag",
        "amount",
    ]

    expected_input_columns = set(column_name_mapping.keys())
    assert (
        set(df.columns).intersection(expected_input_columns) == expected_input_columns
    ), f"Not all columns contained in data. Difference: {expected_input_columns.difference(set(df.columns))}"
    assert (
        df.drop("Description", axis=1).isna().sum().sum() == 0
    ), f"There are NaN values in Toshl data, which is not expected! Please check!"

    df_cleaned = df.copy()
    df_cleaned = df_cleaned.rename(columns=column_name_mapping)[relevant_columns]
    df_cleaned["date"] = pd.to_datetime(df_cleaned["date"], format="%m/%d/%y")
    df_cleaned["expense_amount"] = (
        df_cleaned["expense_amount"].replace(",", "", regex=True).astype("float64")
    )
    df_cleaned["income_amount"] = (
        df_cleaned["income_amount"].replace(",", "", regex=True).astype("float64")
    )
    df_cleaned["amount_main_currency"] = (
        df_cleaned["amount_main_currency"]
        .replace(",", "", regex=True)
        .astype("float64")
    )

    # Create unique amount column depending whether expense_amount is positive (expense) or zero (income)
    df_cleaned["amount"] = pd.Series(
        [
            -y if x > 0.0 else y
            for x, y in zip(
                df_cleaned["expense_amount"], df_cleaned["amount_main_currency"]
            )
        ]
    )
    assert (
        df_cleaned[
            (df_cleaned["income_amount"] != 0.0)
            & (df_cleaned["amount_main_currency"] != df_cleaned["amount"])
        ]
        .count()
        .sum()
        == 0
    ), "Income amount does not match with main currency amount!"
    assert (
        df_cleaned[
            (df_cleaned["expense_amount"] != 0.0)
            & (-df_cleaned["amount_main_currency"] != df_cleaned["amount"])
        ]
        .count()
        .sum()
        == 0
    ), "Expense amount does not match with main currency amount!"

    # All entries that either have category Urlaub or contain Urlaub in the tag field are marked as Urlaub
    df_cleaned.loc[
        (df_cleaned["category"] == "Urlaub")
        | (df_cleaned["tag"].str.contains("Urlaub")),
        "tag",
    ] = "vacation"

    df_cleaned = df_cleaned[output_columns]
    return df_cleaned


def split_cashflow_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Splits whole cashflow data into incomes and expenses, groups it monthly and sums amounts per tag

    Parameters
    ----------
    df  Cleaned cashflow data with columns ["tag", "date", "amount"]

    Returns
    -------
    Tuple of dataframes holding incomes and expenses, each grouped by month
    """
    needed_columns = ["tag", "date", "amount"]
    assert set(needed_columns).intersection(set(df.columns)) == set(
        needed_columns
    ), "Columns missing! Need: {0}, Have: {1}".format(needed_columns, list(df.columns))

    df_grouped = df.groupby([pd.Grouper(key="date", freq="1M"), "tag"]).sum()

    incomes = df_grouped[df_grouped["amount"] > 0.0].copy()
    expenses = df_grouped[df_grouped["amount"] <= 0.0].copy()

    return incomes, expenses


def combine_incomes(
    toshl_income: pd.DataFrame, excel_income: pd.DataFrame
) -> pd.DataFrame:
    """
    Combines two data sources of incomes: toshl incomes and incomes from cashflow excel.

    Parameters
    ----------
    toshl_income: Preprocessed dataframe of toshl incomes
    excel_income: Raw excel income data

    Returns
    -------
    A single dataframe containing all income entries
    """
    df_in = toshl_income.reset_index().copy()

    # Load and clean excel income data
    df_in2 = excel_income.copy()
    df_in2 = (
        df_in2[["Datum", "Art", "Betrag"]]
        .rename(columns={"Datum": "date", "Art": "tag", "Betrag": "amount"})
        .dropna()
    )
    df_in2["date"] = pd.to_datetime(df_in2["date"], format="%d.%m.%Y")

    df_income = pd.concat([df_in, df_in2], ignore_index=True)
    assert (
        df_income.count().iloc[0] == df_in.count().iloc[0] + df_in2.count().iloc[0]
    ), "Some income rows were lost!"

    df_income = df_income.groupby([pd.Grouper(key="date", freq="1M"), "tag"]).sum()

    return df_income


def transform_cashflow_to_wide_format(
    df: pd.DataFrame, tag_category_map: {}
) -> tuple[pd.DataFrame | None, pd.DataFrame]:
    """
    Remap tags of input data to custom categories, and change the format of the dataframe from longlist to wide format
    to easily do computations and plots of the cashflow data.
    Parameters
    ----------
    df: Contains cashflow data as longlist, [date, tag] are indices, amount is only column
    tag_category_map: Mapping of custom categories to a list of toshl tags. Needs to be category map for either income or expenses

    Returns
    -------
    Dataframe in wide format where each column is a category and date column is index
    """
    assert (
        isinstance(df.index, pd.core.indexes.multi.MultiIndex)
        and set(df.index.names) == {"date", "tag"}
        and list(df.columns) == ["amount"]
    ), "Dataframe is not grouped by month with a multi-index of [date, tag] and column amount!"
    ### Define custom categories for all tags of Toshl

    # Create all_category_lists, which is list of category values from custom category map
    # Reduce recursively flattens out all lists and results in one list of categories
    all_category_lists = [cat_list for cat_list in list(tag_category_map.values())]
    category_list = reduce(lambda x, y: x + y, all_category_lists)

    ### Create wide format from longlist, fill NaNs with zero and drop level 0 index "amount"
    pivot_init = df.unstack()
    pivot_init.fillna(0, inplace=True)
    pivot_init.columns = pivot_init.columns.droplevel()

    not_categorized = [tag for tag in pivot_init.columns if tag not in category_list]
    assert (
        len(not_categorized) == 0
    ), f"There are some tags, which are not yet categorized: {not_categorized}"

    # Calculate sum per categories and drop corresponding tags
    pivot = pivot_init.copy()
    for category, category_tags in tag_category_map.items():
        category_tags_in_data = list(
            set(category_tags).intersection(set(pivot.columns))
        )
        pivot[category] = pivot[category_tags_in_data].sum(axis=1)
        # Do not drop the newly created category column in case the custom category has the same name as one of the original ones
        category_columns_to_drop = list(
            set(category_tags_in_data).difference({category})
        )
        pivot.drop(columns=category_columns_to_drop, inplace=True)

    ### Keep only categories with non-zero total amount in result
    category_sum = pivot.sum().reset_index()
    nonzero_categories = list(category_sum[category_sum[0] != 0.0]["tag"])

    pivot = pivot[nonzero_categories]

    return pivot
