"""One-off script to (re-)initialize the ETF master data.

Run this only when the set of held ETFs changes (new position added,
existing position fully closed and no longer relevant, etc.) — the resulting
`source_etf__master_data.csv` and `source_etf__market_snapshot.csv` are
otherwise read as-is by the regular `stocks_main.run_stocks` pipeline.

Usage:
    python -m datahub_stocks.init_master_data
"""

import logging
from pathlib import Path

import pandas as pd

from utils.file_io import load_data
from datahub_stocks.transform.preprocessing import (
    preprocess_buys,
    preprocess_sells,
)
from datahub_stocks.extract.extract_master_data import (
    initialize_master_data,
    initialize_market_snapshot,
)
from constants import DATAHUB_ROOT_FILEPATH


logger = logging.getLogger(__name__)


def init_master_data() -> None:
    filepath_source = Path(DATAHUB_ROOT_FILEPATH) / "source"
    filepath_source_input = filepath_source / "stocks"

    portfolio_path = filepath_source_input / "source_stocks_portfolio_trades.ods"
    etf_buys = preprocess_buys(
        load_data(portfolio_path, file_type="excel", sheet_name="Buys")
    )
    etf_sells = preprocess_sells(
        load_data(portfolio_path, file_type="excel", sheet_name="Sells")
    )
    etf_portfolio = pd.concat([etf_buys, etf_sells], ignore_index=True)
    etf_isin_valid = sorted(set(etf_portfolio["isin"].str.strip()))
    logger.info(f"Initializing master data for {len(etf_isin_valid)} ISINs.")

    initialize_master_data(
        etf_isins=etf_isin_valid,
        out_path=filepath_source / "source_etf__master_data.csv",
    )
    initialize_market_snapshot(
        etf_isins=etf_isin_valid,
        out_path=filepath_source / "source_etf__market_snapshot.csv",
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_master_data()
