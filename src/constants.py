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
PATH_STOCK_MERGERS = STOCKS_INPUT_DIR / "source_stock_mergers.ods"
PATH_ETF_MASTER_DATA = SOURCE_DIR / "source_etf__master_data.csv"
PATH_ETF_MARKET_SNAPSHOT = SOURCE_DIR / "source_etf__market_snapshot.csv"
PATH_ETF_PRICE_CURRENT = SOURCE_DIR / "source_etf__price_current.csv"
PATH_ETF_PRICE_HISTORIC = SOURCE_DIR / "source_etf__price_historic.csv"

# --- Stocks / ETF: transform stage ---
PATH_ETF_SHARES_MONTHLY = TRANSFORM_DIR / "transform_etf__shares_monthly.csv"
PATH_ETF_MONTHLY_INVESTMENTS = TRANSFORM_DIR / "transform_etf__monthly_investments.csv"
PATH_ETF_PORTFOLIO_VALUE = TRANSFORM_DIR / "transform_etf__portfolio_value.csv"

# --- Cashflow: transform stage ---
PATH_CASHFLOW_COMBINED = TRANSFORM_DIR / "transform_cashflow__toshl_cashflow.csv"
PATH_CASHFLOW_INCOMES = TRANSFORM_DIR / "transform_cashflow__incomes.csv"
PATH_CASHFLOW_EXPENSES = TRANSFORM_DIR / "transform_cashflow__expenses.csv"

# --- Cashflow: target stage (dashboard-ready) ---
PATH_CASHFLOW_INCOMES_WIDE = TARGET_DIR / "target_cashflow__incomes.csv"
PATH_CASHFLOW_EXPENSES_WIDE = TARGET_DIR / "target_cashflow__expenses.csv"
