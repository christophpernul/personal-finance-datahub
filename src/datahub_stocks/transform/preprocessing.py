"""Contains preprocessing functionalities for stock and ETF data."""

import pandas as pd


def get_valid_etf_list(
    etf_data: pd.DataFrame, etf_mergers: pd.DataFrame, clean: bool = False
) -> list:
    """Extracts a list of valid ISINs from `etf_data` by dropping ISINs, that were
    merged or converted as of `etf_mergers`."""
    data_isin_column = "isin"
    merger_isin_column = "isin_old"
    assert (
        data_isin_column in etf_data.columns
    ), f"Column `{data_isin_column}` missing in `etf_data`!"
    assert (
        merger_isin_column in etf_mergers.columns
    ), f"Column `{merger_isin_column}` missing in `etf_mergers`!"
    all_isin = list(set(etf_data[data_isin_column]))
    unvalid_isin = list(set(etf_mergers[merger_isin_column]))

    # TODO: The merged new ETF of column "isin_new" should be added!

    valid_isin = [isin for isin in all_isin if isin not in unvalid_isin]
    return [isin.strip() for isin in valid_isin] if clean else valid_isin
