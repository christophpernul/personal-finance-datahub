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


def extract_current_etf_price_data(etf_isins: list) -> pd.DataFrame:
    """Extracts historic price data for relevant etfs."""
    prices = pd.DataFrame(columns=["Date", "Close"])
    for isin in etf_isins:
        try:
            price_isin = yf.Ticker(isin).history(period="1d")
            price_isin["isin"] = isin
        except:
            print(f"Cannot find price data for `{isin}` via yahoo finance!")
            continue
        prices = pd.concat(
            [prices, price_isin[["isin", "Close"]].reset_index()], ignore_index=True
        )
    prices.rename(columns={"Close": "price", "Date": "date"})
    return prices
