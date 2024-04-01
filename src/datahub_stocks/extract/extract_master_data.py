from datetime import datetime
import pandas as pd
import yfinance as yf
from pathlib import Path

from utils.file_io import save_data


def initialize_master_data(etf_isins: list, out_path: Path) -> None:
    """Initializes master data scraped from https://finance.yahoo.com for relevant etfs."""
    etf_info = []
    for isin in etf_isins:
        try:
            info = yf.Ticker(isin).info
        except:
            print(f"Cannot find `{isin}` via yahoo finance!")
            etf_info.append(
                {"isin": isin, "name": "", "symbol": "", "type": "", "currency": ""}
            )
            continue
        etf_info.append(
            {
                "isin": isin,
                "name": info.get("longName"),
                "symbol": info.get("symbol"),
                "type": info.get("quoteType"),
                "currency": info.get("currency"),
            }
        )
    master_data = pd.DataFrame(etf_info).sort_values(by="name")
    save_data(
        data=master_data,
        filepath=out_path,
    )
    print(f"Initialized `{out_path}` with {len(etf_info)} entries.")


def extract_current_etf_prices(etfs: list) -> pd.DataFrame:
    """Extracts historic price data for relevant etfs."""
    prices = pd.DataFrame(columns=["isin", "Date", "Close"])
    for isin in etfs:
        try:
            price_isin = yf.Ticker(isin).history(period="1d")
            price_isin["isin"] = isin
        except:
            print(f"Cannot find price data for `{isin}` via yahoo finance!")
            continue
        prices = pd.concat(
            [prices, price_isin[["isin", "Close"]].reset_index()],
            ignore_index=True,
        )
    # Returned dataframe from yahoo contains columns Close, and Date after resetting index
    prices.rename(
        columns={
            "Close": "price",
            "Date": "date",
        },
        inplace=True,
    )
    return prices


def extract_historic_etf_prices(etfs: pd.DataFrame) -> pd.DataFrame:
    """Extracts historic price data for relevant etfs."""
    current_date = datetime.now()
    current_date_string = current_date.strftime("%Y-%m-%d")

    symbols = list(etfs["symbol"])

    historic_prices = pd.DataFrame(columns=["date", "isin", "price"])
    for idx, row in etfs.iterrows():
        isin = row["isin"]
        symbol = row["symbol"]
        try:
            data = (
                yf.download(
                    symbol,
                    start="2020-01-01",
                    end=current_date_string,
                    interval="1mo",
                )
                .reset_index()[["Date", "Adj Close"]]
                .rename(columns={"Date": "date", "Adj Close": "price"})
            )
        except:
            print(f"Cannot find price data for `{isin}` via yahoo finance!")
            continue
        data["isin"] = isin
        historic_prices = pd.concat(
            [historic_prices, data],
            ignore_index=True,
        )
    return historic_prices
