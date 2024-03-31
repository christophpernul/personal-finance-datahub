import pandas as pd
import pandas.testing as pd_testing
import pytest

from src.datahub_stocks.transform.preprocessing import get_valid_etf_list

ISINS = ["  apple", "a banana  ", "  and a carrot  ", "date", "date"]


@pytest.fixture
def etf():
    # Create a test DataFrame
    df = pd.DataFrame(
        {
            "ISIN": ISINS,
            "other": ["" for _ in ISINS],
        }
    )
    return df


@pytest.fixture
def etf_merger():
    # Create a test DataFrame
    df = pd.DataFrame(
        {
            "isin_old": ["  and a carrot  "],
            "type": ["remove"],
        }
    )
    return df


@pytest.mark.ut
def test_get_valid_etf_list(etf, etf_merger):
    expectation = list(set([isin for isin in ISINS if isin != "  and a carrot  "]))
    result = get_valid_etf_list(etf_data=etf, etf_mergers=etf_merger)
    assert result == expectation


@pytest.mark.ut
def test_get_valid_etf_list_clean(etf, etf_merger):
    expectation = list(
        set([isin.strip() for isin in ISINS if isin != "  and a carrot  "])
    )
    result = get_valid_etf_list(etf_data=etf, etf_mergers=etf_merger, clean=True)
    assert result == expectation
