from datetime import datetime, date
import re
import pandas as pd
import yfinance as yf
from pathlib import Path
import requests

from utils.file_io import save_data

_FRANKFURTER_URL = "https://api.frankfurter.app/latest"
_FALLBACK_USD_TO_EUR = 0.93

_ACC_PATTERN = re.compile(
    r"\b(?:acc|accumulating|1c|thesaurierend)\b|\(c\)",
    re.IGNORECASE,
)
_DIST_PATTERN = re.compile(
    r"\b(?:dist|distributing|1d|ausschüttend)\b|\(d\)",
    re.IGNORECASE,
)


def _infer_distribution(name: str) -> str:
    """Guess whether an ETF is accumulating or distributing from its name.

    European UCITS providers usually encode the distribution policy in the
    name (`(Acc)`, `1C`, `(Dist)`, `1D`, `Thesaurierend`, ...). Returns an
    empty string when no marker is found so the user can fill it in manually.
    """
    if not name:
        return ""
    if _ACC_PATTERN.search(name):
        return "accumulating"
    if _DIST_PATTERN.search(name):
        return "distributing"
    return ""


def fetch_conversion_rate_usdollar_euro(dollar_to_euro=True) -> float:
    """
    Fetches the current USD/EUR exchange rate from api.frankfurter.app (ECB data).
    Falls back to a hardcoded rate if the request fails.
    :param dollar_to_euro: if True, returns USD→EUR rate; otherwise EUR→USD.
    :return: conversion rate as float
    """
    from_currency, to_currency = ("USD", "EUR") if dollar_to_euro else ("EUR", "USD")
    try:
        r = requests.get(
            _FRANKFURTER_URL,
            params={"from": from_currency, "to": to_currency},
            timeout=10,
        )
        r.raise_for_status()
        print(
            f"Fetched {from_currency}/{to_currency} rate from Frankfurter API: {r.json()['rates'][to_currency]}"
        )
        return float(r.json()["rates"][to_currency])
    except Exception:
        fallback = (
            _FALLBACK_USD_TO_EUR
            if dollar_to_euro
            else round(1 / _FALLBACK_USD_TO_EUR, 6)
        )
        print(
            f"Warning: Could not fetch {from_currency}/{to_currency} rate, using fallback {fallback}."
        )
        return fallback


def initialize_master_data(etf_isins: list, out_path: Path) -> None:
    """Fetch static ETF attributes from Yahoo Finance and write them to `out_path`.

    Columns `replication`, `etf_type` and `comment` are left empty and must be
    filled in manually. Rows for ISINs that Yahoo cannot resolve are kept with
    empty values so the failures stay visible.
    """
    rows = []
    for isin in etf_isins:
        try:
            info = yf.Ticker(isin).info
        except Exception:
            print(f"Cannot find `{isin}` via yahoo finance!")
            rows.append(
                {
                    "isin": isin,
                    "name": "",
                    "symbol": "",
                    "type": "",
                    "currency": "",
                    "exchange_name": "",
                    "ter": None,
                    "region": "",
                    "distribution": "",
                    "replication": "",
                    "etf_type": "",
                    "comment": "",
                }
            )
            continue
        name = info.get("longName") or info.get("shortName") or ""
        rows.append(
            {
                "isin": isin,
                "name": name,
                "symbol": info.get("symbol"),
                "type": info.get("quoteType"),
                "currency": info.get("currency"),
                "exchange_name": info.get("fullExchangeName"),
                "ter": info.get("netExpenseRatio"),
                "region": info.get("region"),
                "distribution": _infer_distribution(name),
                "replication": "",
                "etf_type": "",
                "comment": "",
            }
        )
    master_data = pd.DataFrame(rows).sort_values(by="name")
    save_data(data=master_data, filepath=out_path)
    print(f"Initialized `{out_path}` with {len(rows)} entries.")


def initialize_market_snapshot(etf_isins: list, out_path: Path) -> None:
    """Fetch a Yahoo market-data snapshot for the given ISINs and write it to
    `out_path`.

    Captures 52-week highs/lows, all-time highs/lows, moving averages, last
    close price and regular market volume. Not consumed by the regular
    pipeline — kept around for later ad-hoc analyses.
    """
    rows = []
    for isin in etf_isins:
        try:
            info = yf.Ticker(isin).info
        except Exception:
            print(f"Cannot find `{isin}` via yahoo finance!")
            continue
        rows.append(
            {
                "isin": isin,
                "last_close_price": info.get("previousClose"),
                "reg_market_volume": info.get("regularMarketVolume"),
                "low_52_week": info.get("fiftyTwoWeekLow"),
                "high_52_week": info.get("fiftyTwoWeekHigh"),
                "all_time_low": info.get("allTimeLow"),
                "all_time_high": info.get("allTimeHigh"),
                "fifty_day_avg": info.get("fiftyDayAverage"),
                "two_hundred_day_avg": info.get("twoHundredDayAverage"),
            }
        )
    market_data = pd.DataFrame(rows).sort_values(by="isin")
    save_data(data=market_data, filepath=out_path)
    print(f"Initialized `{out_path}` with {len(rows)} entries.")


def _fetch_latest_price(symbol: str) -> float | None:
    """Return the most recent close for `symbol`, or None if unavailable.

    Tries `fast_info.last_price` first (cheapest call), then the last `Close`
    from a 5-day history window, and finally falls back to the quote-summary
    endpoint via `Ticker.info` — needed for ETFs listed only on illiquid
    exchanges (e.g. `.SG`) that Yahoo carries a quote for but no chart bars.
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

    try:
        info = yf.Ticker(symbol).info
        price = info.get("regularMarketPrice") or info.get("previousClose")
        if price is not None and not pd.isna(price):
            return float(price)
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
            print(
                f"Converting price `{price}` for `{isin}` from USD to EUR using rate {usd_to_eur}."
            )
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
                start="2019-01-01",
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
