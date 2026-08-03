"""Contains constant global variables used in the datahub.

All datahub file locations follow the flat, staged naming convention
``<stage>_<type>__<name>.csv`` where ``stage`` is ``source``/``transform``/
``target`` and ``type`` is the data domain (``cashflow``, ``stocks``,
``etf`` for portfolio data).
"""

from pathlib import Path

DATAHUB_ROOT_FILEPATH = "D:/SynologyDrive/Finance/data/datahub/"
DATAHUB_ROOT = Path(DATAHUB_ROOT_FILEPATH)

TOSHL_CATEGORY_MAP = "toshl_tag_categorization.json"
TOSHL_SOURCE_FILEPATTERN = "bilanz_*.csv"

# --- Stage directories (flat; the filename encodes the data domain) ---
SOURCE_DIR = DATAHUB_ROOT / "source"
TRANSFORM_DIR = DATAHUB_ROOT / "transform"
TARGET_DIR = DATAHUB_ROOT / "target"

# --- Manually maintained source inputs (kept in per-domain subfolders) ---
STOCKS_INPUT_DIR = SOURCE_DIR / "stocks"
TOSHL_SOURCE_DIR = SOURCE_DIR / "cashflow" / "toshl"

# --- Stocks / ETF: source stage ---
PATH_STOCKS_TRADES = STOCKS_INPUT_DIR / "source_stocks_portfolio_trades.ods"
# Individual (single) stock trades, e.g. Coca Cola, Nvidia. Same sheet layout as
# the ETF portfolio file but the securities carry no ISIN.
PATH_SINGLE_STOCKS_TRADES = STOCKS_INPUT_DIR / "source_stocks_trades.ods"
# Manually maintained master data for individual stocks (keyed by name, since
# stocks carry no ISIN): yahoo ticker symbol and currency for price extraction.
PATH_STOCKS_MASTER_DATA = STOCKS_INPUT_DIR / "source_stocks__master_data.csv"
PATH_STOCK_MERGERS = STOCKS_INPUT_DIR / "source_stock_mergers.ods"
# Manually maintained stock splits (keyed by name); shares held before the split
# date are multiplied by split_to / split_from.
PATH_STOCK_SPLITS = STOCKS_INPUT_DIR / "source_stock_splits.ods"
# Manually maintained investment-cost report: comdirect yearly cost reports
# (per-ETF Wertpapierkosten + Depotentgelt) combined with Trade Republic fees.
# Columns: Datum,Kosten,Anbieter,Kommentar,Name,ISIN,Note (comma-separated,
# dot decimals, German dd.mm.yyyy dates, costs stored as positive amounts).
# Feeds the `Investment Costs` expense category in the cashflow target table.
PATH_INVESTMENT_COSTS = STOCKS_INPUT_DIR / "comdirect_costs_combined.csv"
PATH_ETF_MASTER_DATA = SOURCE_DIR / "source_etf__master_data.csv"
PATH_ETF_MARKET_SNAPSHOT = SOURCE_DIR / "source_etf__market_snapshot.csv"
PATH_ETF_PRICE_CURRENT = SOURCE_DIR / "source_etf__price_current.csv"
PATH_ETF_PRICE_HISTORIC = SOURCE_DIR / "source_etf__price_historic.csv"

# --- Stocks / ETF: transform stage ---
PATH_ETF_SHARES_MONTHLY = TRANSFORM_DIR / "transform_etf__shares_monthly.csv"
PATH_ETF_MONTHLY_INVESTMENTS = TRANSFORM_DIR / "transform_etf__monthly_investments.csv"
# Monthly *net* rebalancing cashflow (sell proceeds - buy spend - order fees),
# split into an expense/income column; kept apart from the gross monthly
# investments because rebalancing sells fund the rebalancing buys.
PATH_ETF_MONTHLY_REBALANCING = TRANSFORM_DIR / "transform_etf__monthly_rebalancing.csv"
PATH_ETF_MONTHLY_DIVIDENDS = TRANSFORM_DIR / "transform_etf__monthly_dividends.csv"
# Monthly received interest on cash deposits, split into the two account kinds
# (Tagesgeld / Festgeld); sourced from the `Zinsen` sheet of the trades file.
PATH_MONTHLY_INTEREST = TRANSFORM_DIR / "transform_stocks__monthly_interest.csv"
PATH_ETF_PORTFOLIO_VALUE = TRANSFORM_DIR / "transform_etf__portfolio_value.csv"

# Individual stocks: transform stage (mirrors the ETF portfolio tables, but the
# securities are identified by name instead of ISIN)
PATH_STOCKS_SHARES_MONTHLY = TRANSFORM_DIR / "transform_stocks__shares_monthly.csv"
PATH_STOCKS_PORTFOLIO_VALUE = TRANSFORM_DIR / "transform_stocks__portfolio_value.csv"

# Combined ETF + individual-stock portfolio value with a `security_type` column
PATH_PORTFOLIO_VALUE_COMBINED = TRANSFORM_DIR / "transform_portfolio__value.csv"

# --- Cashflow: transform stage ---
PATH_CASHFLOW_COMBINED = TRANSFORM_DIR / "transform_cashflow__toshl_cashflow.csv"
PATH_CASHFLOW_INCOMES = TRANSFORM_DIR / "transform_cashflow__incomes.csv"
PATH_CASHFLOW_EXPENSES = TRANSFORM_DIR / "transform_cashflow__expenses.csv"

# --- Cashflow: target stage (dashboard-ready) ---
PATH_CASHFLOW_INCOMES_WIDE = TARGET_DIR / "target_cashflow__incomes.csv"
PATH_CASHFLOW_EXPENSES_WIDE = TARGET_DIR / "target_cashflow__expenses.csv"
