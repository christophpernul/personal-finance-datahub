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

# --- Manual transactions (all domains) ---
# The single manually maintained file with every correction of the source data,
# so no concrete booking ever lives in the code. Columns (German locale):
# action;date;exchange;type;coin;amount;fee;group;refid;tag;description;comment
# `action` decides which columns are used, see MANUAL_ACTIONS.
PATH_MANUAL_TRANSACTIONS = SOURCE_DIR / "source_manual__transactions.csv"
MANUAL_ACTION_CRYPTO_TRANSACTION = "crypto_transaction"
MANUAL_ACTION_CRYPTO_CONVERSION = "crypto_conversion"
MANUAL_ACTION_TOSHL_EXCLUSION = "toshl_exclusion"
MANUAL_ACTIONS = {
    # A coin movement missing in the exports; uses date, exchange, type, coin,
    # amount, fee, group (legs of one manual trade share a group)
    MANUAL_ACTION_CRYPTO_TRANSACTION,
    # An exported booking that is one side of a conversion done by the exchange
    # (e.g. on delisting); uses exchange, refid (of the exported booking) and
    # group (both sides of one conversion share a group)
    MANUAL_ACTION_CRYPTO_CONVERSION,
    # A toshl booking that another source already books; uses date, tag,
    # amount (positive, as shown in toshl) and description
    MANUAL_ACTION_TOSHL_EXCLUSION,
}

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
# Monthly Riester pension contributions (an expense), summed from the
# `Riester Buy` sheet of the trades file.
PATH_MONTHLY_RIESTER = TRANSFORM_DIR / "transform_stocks__monthly_riester.csv"
PATH_ETF_PORTFOLIO_VALUE = TRANSFORM_DIR / "transform_etf__portfolio_value.csv"

# Individual stocks: transform stage (mirrors the ETF portfolio tables, but the
# securities are identified by name instead of ISIN)
PATH_STOCKS_SHARES_MONTHLY = TRANSFORM_DIR / "transform_stocks__shares_monthly.csv"
PATH_STOCKS_PORTFOLIO_VALUE = TRANSFORM_DIR / "transform_stocks__portfolio_value.csv"

# Combined ETF + individual-stock portfolio value with a `security_type` column
PATH_PORTFOLIO_VALUE_COMBINED = TRANSFORM_DIR / "transform_portfolio__value.csv"

# --- Crypto: source stage ---
# Raw exchange exports, picked up by file pattern so newer exports only need to
# be dropped into the folder (rows of overlapping exports are deduplicated).
CRYPTO_INPUT_DIR = SOURCE_DIR / "crypto"
# Kraken switched from `kraken_ledgers_*` to `kraken-ledgers-*` file names
CRYPTO_KRAKEN_LEDGERS_FILEPATTERN = "kraken[-_]ledgers[-_]*.csv"
CRYPTO_BINANCE_TRADES_FILEPATTERN = "binance_tradehistory_*.csv"
CRYPTO_BINANCE_DEPOSITS_FILEPATTERN = "binance_deposits_*.xlsx"
CRYPTO_KUCOIN_TRADES_FILEPATTERN = "kucoin_trades_*.csv"
CRYPTO_KUCOIN_DEPOSITS_FILEPATTERN = "kucoin_deposits_*.csv"
# Manually maintained coin master data: coin;name;symbol;currency, where symbol
# is the yahoo ticker (e.g. BTC-EUR) and currency the quote currency of it.
PATH_CRYPTO_MASTER_DATA = CRYPTO_INPUT_DIR / "source_crypto__master_data.csv"

# --- Crypto: transform stage ---
# Every balance change of every coin per exchange/wallet, see
# `datahub_crypto.transform.preprocessing.LEDGER_COLUMNS`
PATH_CRYPTO_LEDGER = TRANSFORM_DIR / "transform_crypto__ledger.csv"
# One row per trade with the moved cost basis and fees in EUR
PATH_CRYPTO_TRADES = TRANSFORM_DIR / "transform_crypto__trades.csv"
PATH_CRYPTO_HOLDINGS_MONTHLY = TRANSFORM_DIR / "transform_crypto__holdings_monthly.csv"
# Current value of every held coin per exchange/wallet (portfolio page)
PATH_CRYPTO_PORTFOLIO_VALUE = TRANSFORM_DIR / "transform_crypto__portfolio_value.csv"
# One row per coin ever held, incl. sold coins and their realized gain
PATH_CRYPTO_COINS = TRANSFORM_DIR / "transform_crypto__coins.csv"

# --- Cashflow: transform stage ---
PATH_CASHFLOW_COMBINED = TRANSFORM_DIR / "transform_cashflow__toshl_cashflow.csv"
# Toshl categories whose bookings are taken from another datahub source instead
# (crypto deposits/withdrawals come from the exchange exports)
TOSHL_CATEGORIES_FROM_OTHER_SOURCES = ["Crypto"]
# The toshl bookings set aside because of the above, kept for reconciliation
# together with the reason why each was set aside
PATH_CASHFLOW_TOSHL_REPLACED = (
    TRANSFORM_DIR / "transform_cashflow__toshl_replaced_bookings.csv"
)
# Single toshl transactions, split by the sign of their own amount
PATH_CASHFLOW_INCOMES = TRANSFORM_DIR / "transform_cashflow__incomes.csv"
PATH_CASHFLOW_EXPENSES = TRANSFORM_DIR / "transform_cashflow__expenses.csv"
# The same transactions summed per month and tag
PATH_CASHFLOW_INCOMES_MONTHLY = (
    TRANSFORM_DIR / "transform_cashflow__incomes_monthly.csv"
)
PATH_CASHFLOW_EXPENSES_MONTHLY = (
    TRANSFORM_DIR / "transform_cashflow__expenses_monthly.csv"
)

# --- Cashflow: target stage (dashboard-ready) ---
# Single transactions across all sources (toshl, ETF trades, dividends,
# interest, Riester, investment costs), columns [date, tag, category, amount]
PATH_CASHFLOW_INCOMES_TARGET = TARGET_DIR / "target_cashflow__incomes.csv"
PATH_CASHFLOW_EXPENSES_TARGET = TARGET_DIR / "target_cashflow__expenses.csv"
# The same transactions aggregated per month, one column per category
PATH_CASHFLOW_INCOMES_WIDE = TARGET_DIR / "target_cashflow__incomes_monthly.csv"
PATH_CASHFLOW_EXPENSES_WIDE = TARGET_DIR / "target_cashflow__expenses_monthly.csv"
