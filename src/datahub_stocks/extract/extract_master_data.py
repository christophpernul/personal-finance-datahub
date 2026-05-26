import json
from datetime import datetime, date
import pandas as pd
import yfinance as yf
from pathlib import Path
import requests

from utils.file_io import save_data


def fetch_conversion_rate_usdollar_euro(dollar_to_euro=True) -> float:
    """
    Uses https://www.finanzen.net to convert US-dollars $ to Euro € and vice versa.
    :param dollar_to_euro: boolean flag, whether to convert dollars to euro
    :return: conversion rate
    """
    date_today = date.today().strftime(format="%Y-%m-%d")
    if dollar_to_euro == True:
        url = f"https://www.finanzen.net/ajax/currencyConverter_Exchangerate/USD/EUR/{date_today}"
    else:
        url = f"https://www.finanzen.net/ajax/currencyConverter_Exchangerate/EUR/USD/{date_today}"

    r = requests.post(url)
    # TODO: Find new API, e.g. https://public.opendatasoft.com/api/explore/v2.1/console
    # assert r.status_code == requests.codes.ok, "Could not convert $ to €!"
    conversion = 0.93  # float(json.loads(r.content)[0])

    return conversion


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


def _fetch_latest_price(symbol: str) -> float | None:
    """Return the most recent close for `symbol`, or None if unavailable.

    Tries `fast_info.last_price` first (cheapest call); falls back to the
    last `Close` from a 5-day history window, which is more robust for
    ETFs where fast_info is missing or stale.
    """
    try:
        price = yf.Ticker(symbol).fast_info.last_price
        if price is not None and not pd.isna(price):
            return float(price)
    except Exception:
        pass

    try:
        hist = yf.Ticker(symbol).history(period="5d", auto_adjust=False)
        if not hist.empty:
            return float(hist["Close"].dropna().iloc[-1])
    except Exception:
        pass

    return None


def extract_current_etf_prices(etfs: pd.DataFrame) -> pd.DataFrame:
    """Extracts the latest price for each etf via yahoo finance.

    Yahoo's price endpoints accept ticker symbols, not ISINs, so the
    resolved `symbol` from the master data must be used. Prices quoted
    in USD are converted to EUR; prices already quoted in EUR (or other
    currencies) are kept as-is.

    :param etfs: DataFrame with columns `isin`, `symbol`, `currency`.
    :return: DataFrame with columns `isin`, `date`, `price` (in EUR).
    """
    today = date.today()
    usd_to_eur = fetch_conversion_rate_usdollar_euro()

    rows = []
    for _, row in etfs.iterrows():
        isin = row["isin"]
        symbol = row["symbol"]
        currency = row.get("currency")

        if not symbol or pd.isna(symbol):
            print(f"No yahoo symbol available for `{isin}`, skipping.")
            continue

        price = _fetch_latest_price(symbol)
        if price is None:
            print(f"Cannot find price data for `{isin}` ({symbol}) via yahoo finance!")
            continue

        if currency == "USD":
            price = price * usd_to_eur

        rows.append({"isin": isin, "date": today, "price": price})

    return pd.DataFrame(rows, columns=["isin", "date", "price"])


def extract_historic_etf_prices(etfs: pd.DataFrame) -> pd.DataFrame:
    """Extracts historic (monthly) price data for relevant etfs.

    Uses `Ticker.history()` which always returns flat (non-multi-indexed)
    columns, and explicitly disables auto-adjust so the `Close` column
    is the unadjusted close price. USD-quoted prices are converted to EUR.

    :param etfs: DataFrame with columns `isin`, `symbol`, `currency`.
    :return: DataFrame with columns `date`, `isin`, `price` (in EUR).
    """
    current_date_string = datetime.now().strftime("%Y-%m-%d")
    usd_to_eur = fetch_conversion_rate_usdollar_euro()

    frames = []
    for _, row in etfs.iterrows():
        isin = row["isin"]
        symbol = row["symbol"]
        currency = row.get("currency")

        if not symbol or pd.isna(symbol):
            print(f"No yahoo symbol available for `{isin}`, skipping.")
            continue

        try:
            data = yf.Ticker(symbol).history(
                start="2020-01-01",
                end=current_date_string,
                interval="1mo",
                auto_adjust=False,
            )
        except Exception as exc:
            print(
                f"Cannot find historic data for `{isin}` ({symbol}) via yahoo finance: {exc}"
            )
            continue

        if data.empty or "Close" not in data.columns:
            print(f"No historic data returned for `{isin}` ({symbol}), skipping.")
            continue

        data = (
            data.reset_index()[["Date", "Close"]]
            .rename(columns={"Date": "date", "Close": "price"})
            .dropna(subset=["price"])
        )
        data["isin"] = isin
        if currency == "USD":
            data["price"] = data["price"] * usd_to_eur
        frames.append(data[["date", "isin", "price"]])

    if not frames:
        return pd.DataFrame(columns=["date", "isin", "price"])
    return pd.concat(frames, ignore_index=True)
