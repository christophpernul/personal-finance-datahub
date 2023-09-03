import pandas as pd
from pathlib import Path
from typing import Dict

from src.datahub_library.file_handling_lib import get_datahub_config, load_data


def update_toshl_cashflow(datahub_source_root_path: Path, datahub_cashflow_config: Dict) -> pd.DataFrame:
    filepath_toshl = datahub_source_root_path / datahub_cashflow_config["filePath"] / \
                     datahub_cashflow_config["cashflowHub"]["filePath"]
    raw_data_filepattern = datahub_cashflow_config["cashflowHub"]["sourceFilePattern"]

    raw_data_files: [Path] = sorted(filepath_toshl.glob(raw_data_filepattern))
    for cnt, raw_file_path in enumerate(raw_data_files):
        df = load_data(raw_file_path)
        assert df.drop("Description", axis=1).isna().sum().sum() == 0, f"There are NaN values in Toshl data!"
        if cnt == 0:
            df_cashflow = df.copy()
        else:
            df_cashflow = pd.concat([df_cashflow, df], ignore_index=True)
        print(f"File {raw_file_path.name} contains {df.count()[0]} rows.")
    return df_cashflow


def cleaning_cashflow(df_input: pd.DataFrame) -> pd.DataFrame:
    """
    Data cleaning and preprocessing of cashflow data.
    :param df_input: Multiple toshl monthly-exports appended into a single dataframe
    :return: preprocessed dataframe
    """
    assert df_input.drop("Description", axis=1).isna().sum().sum() == 0, f"There are NaN values in Toshl data!"
    ### Data cleaning
    df_init = df_input.copy()
    df_init['Date'] = pd.to_datetime(df_init['Date'], format='%m/%d/%y')
    df_init.drop(columns=['Account', 'Currency', 'Main currency', 'Description'], inplace=True)
    df_init['Expense amount'] = df_init['Expense amount'].str.replace(',', '')
    df_init['Income amount'] = df_init['Income amount'].str.replace(',', '').astype('float64')
    df_init['In main currency'] = df_init['In main currency'].str.replace(',', '')
    df_init['Expense amount'] = df_init['Expense amount'].astype('float64')
    df_init['In main currency'] = df_init['In main currency'].astype('float64')

    ### Preprocessing of cashflow amounts
    df_init['Amount'] = pd.Series([-y if x > 0. else y
                                   for x, y in zip(df_init['Expense amount'],
                                                   df_init['In main currency']
                                                   )
                                   ]
                                  )
    # TODO: What are these assertions doing?!
    # assert df_init[(~df_init["Income amount"].isin(["0.0", "0"])) &
    #                (df_init["In main currency"] != df_init["Amount"])
    #                ].count().sum() == 0, "Income amount does not match with main currency amount!"
    # assert df_init[(~df_init["Expense amount"].isin(["0.0", "0"])) &
    #                (-df_init["In main currency"] != df_init["Amount"])
    #                ].count().sum() == 0, "Expense amount does not match with main currency amount!"

    ### Remap all tags with category "Urlaub" to "old-tag, Urlaub" and map afterwards all double-tags
    ### containing "Urlaub" to the Urlaub tag
    df_init.loc[df_init["Category"] == "Urlaub", "Tags"] = df_init["Tags"].apply(lambda tag: tag + ", Urlaub")
    df_init["split_tags"] = df_init["Tags"].apply(lambda x: x.split(","))
    assert df_init[df_init["split_tags"].apply(len) > 1]["split_tags"].apply(lambda x: \
                                                                                 "Urlaub" in [s.strip() for s in x]
                                                                             ).all() == True, \
        'Some entries with multiple tags do not contain "Urlaub"! Mapping not possible!'
    df_init.loc[df_init["split_tags"].apply(len) > 1, "Tags"] = "Urlaub"

    df_init = df_init[["Date", "Category", "Tags", "Amount"]]
    return df_init


def split_cashflow_data(df_cleaned: pd.DataFrame) -> pd.DataFrame:
    """
    Splits whole cashflow data into incomes and expenses and groups it monthly and sums amounts per tag
    :param df_cleaned: Cleaned dataframe of cashflow
    :return: Tuple of dataframes holding incomes and expenses, each grouped by month
    """
    needed_columns = ["Tags", "Date", "Amount"]
    assert set(needed_columns).intersection(set(df_cleaned.columns)) == set(needed_columns), \
        "Columns missing! Need: {0}, Have: {1}".format(needed_columns, list(df_cleaned.columns))

    df_grouped = df_cleaned.groupby([pd.Grouper(key='Date', freq='1M'), 'Tags']).sum()

    incomes = df_grouped[df_grouped["Amount"] > 0.].copy()
    expenses = df_grouped[df_grouped["Amount"] <= 0.].copy()

    return (incomes, expenses)


def combine_incomes(toshl_income, excel_income):
    """
    Combines two data sources of incomes: toshl incomes and incomes from cashflow excel.
    :param toshl_income: Preprocessed dataframe of toshl incomes (after cleaning and splitting)
    :param excel_income: Raw excel income data
    :return: Total income data
    """
    df_in = toshl_income.reset_index().copy()
    df_in["Tags"] = df_in["Tags"].apply(lambda x: "Salary" if x in ["Privat", "NHK", "OL"] else x)

    df_in2 = excel_income.copy()
    df_in2 = df_in2[["Datum", "Art", "Betrag"]].rename(columns={"Datum": "Date",
                                                                "Art": "Tags",
                                                                "Betrag": "Amount"}).dropna()
    df_in2["Date"] = pd.to_datetime(df_in2["Date"], format="%d.%m.%Y")
    df_in2["Tags"] = df_in2["Tags"].apply(lambda x: "Salary" if x in ["Gehalt", "Sodexo"] else x)

    df_income = pd.concat([df_in, df_in2], ignore_index=True)
    assert df_income.count()[0] == df_in.count()[0] + df_in2.count()[0], "Some income rows were lost!"

    df_income = df_income.groupby([pd.Grouper(key='Date', freq='1M'), 'Tags']).sum()

    return (df_income)


def preprocess_cashflow(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remap tags of input data to custom categories, and change the format of the dataframe in order to
    easily to computations and plots of the cashflow data.
    :param df: Dataframe, holding either incomes or expenses (cleaned) and grouped by month (tags as rows)
    :return: dataframe, where each row consists of cashflow data of of a month, each column represents a
            custom category
    """
    # TODO: Use three different checks for this to know what is the issue! AND check why Category is in there now!
    assert isinstance(df.index, pd.core.indexes.multi.MultiIndex) and \
           set(df.index.names) == set(["Date", "Tags"]) and \
           list(df.columns) == ["Category", "Amount"], "Dataframe is not grouped by month!"
    ### Define custom categories for all tags of Toshl: Make sure category names differ from tag-names,
    ### otherwise column is dropped and aggregate is wrong
    category_dict = {
        "home": ['rent', 'insurance', 'Miete'],
        "food_healthy": ['restaurants', 'Lebensmittel', 'groceries', 'Restaurants', 'Restaurant Mittag'],
        "food_unhealthy": ['Fast Food', 'Süßigkeiten'],
        "alcoholic_drinks": ['alcohol', 'Alkohol'],
        "non-alcoholic_drinks": ['Kaffee und Tee', 'Erfrischungsgetränke', 'coffee & tea', 'soft drinks'],
        "travel_vacation": ['sightseeing', 'Sightseeing', 'Beherbergung', 'accommodation', 'Urlaub'],
        "transportation": ['bus', 'Bus', 'taxi', 'Taxi', 'metro', 'Metro', 'Eisenbahn', 'train', 'car',
                           'Auto', 'parking', 'airplane', 'fuel', 'Flugzeug'],
        "sports": ['training', 'Training', 'MoTu', 'Turnier', 'sport equipment', 'Billard', 'Konsum Training'],
        "events_leisure_books_abos": ['events', 'Events', 'adult fun', 'Spaß für Erwachsene', 'games', 'sport venues',
                                      'membership fees', 'apps', 'music', 'books'],
        "clothes_medicine": ['clothes', 'accessories', 'cosmetics', 'medicine', 'hairdresser',
                             'medical services', 'medical servies', "shoes"],
        "private_devices": ['devices', 'bike', 'bicycle', 'movies & TV', 'mobile phone', 'home improvement',
                            'internet', 'landline phone', 'furniture'],
        "presents": ['birthday', 'X-Mas'],
        "other": ['wechsel', 'income tax', 'tuition', 'publications', 'Spende'],
        "stocks": ['equity purchase'],
        #### Income categories
        "compensation_caution": ["Entschädigung"],
        "salary": ["Salary", "Gehalt Vorschuss", "Reisekosten"],
        "present": ["Geschenk"],
        "tax_compensation": ["Kirchensteuer Erstattung", "Steuerausgleich"],
        "investment_profit": ["Investing"]
    }
    from functools import reduce
    category_list = reduce(lambda x, y: x + y, category_dict.values())

    ### Need another format of the table, fill NaNs with zero and drop level 0 index "Amount"
    pivot_init = df.unstack()
    pivot_init.fillna(0, inplace=True)
    pivot_init.columns = pivot_init.columns.droplevel()

    #### Extract expenses and incomes from building-upkeep (caution) when switching flats
    if 'building upkeep' in pivot_init.columns:
        building_upkeep = pivot_init['building upkeep']
        pivot_init.drop(columns=['building upkeep'], inplace=True)
    elif 'Wechsel' in pivot_init.columns:
        building_upkeep = pivot_init['Wechsel']
        pivot_init.drop(columns=['Wechsel'], inplace=True)
    else:
        building_upkeep = None

    ### Apply custom category definition to dataframe
    not_categorized = [tag for tag in pivot_init.columns if tag not in category_list]
    assert len(not_categorized) == 0, "There are some tags, which are not yet categorized: {}".format(not_categorized)

    pivot = pivot_init.copy()
    for category, tag_list in category_dict.items():
        tag_list_in_data = list(set(tag_list).intersection(set(pivot.columns)))
        pivot[category] = pivot[tag_list_in_data].sum(axis=1)
        pivot.drop(columns=tag_list_in_data, inplace=True)

    ### Keep only categories with non-zero total amount in dataframe
    category_sum = pivot.sum().reset_index()
    nonzero_categories = list(category_sum[category_sum[0] != 0.]["Tags"])

    pivot = pivot[nonzero_categories]

    return ((building_upkeep, pivot))


if __name__ == "__main__":
    datahub_config = get_datahub_config()
    config_dh = datahub_config["datahubMeta"]
    config_cashflow_dh = datahub_config["datahubCashflowMeta"]
    filepath_source_root = Path(config_dh["datahubRootFilepath"]) / config_dh["sourceLayerName"]

    df_cashflow = update_toshl_cashflow(datahub_source_root_path=filepath_source_root,
                                        datahub_cashflow_config=config_cashflow_dh,
                                        )
    print("All done!")

    df_cashflow = pl.cleaning_cashflow(df_cashflow_init)
    (incomes, expenses) = pl.split_cashflow_data(df_cashflow)
    (caution_expenses, df_expenses) = pl.preprocess_cashflow(expenses)
    df_income_total = pl.combine_incomes(incomes, df_income_init)
    (caution_income, df_incomes) = pl.preprocess_cashflow(df_income_total)
