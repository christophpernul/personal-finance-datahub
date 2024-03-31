import logging
from pathlib import Path

from utils.file_io import get_config_file, save_data, load_data
from transform.preprocessing import get_valid_etf_list
from utils.datacleaning import convert_columns_to_timestamp
from extract.extract_master_data import (
    initialize_master_data,
    extract_current_etf_price_data,
)

from constants import DATAHUB_ROOT_FILEPATH


logger = logging.getLogger(__name__)


def run_stocks(init: bool = False):
    filepath_source = Path(DATAHUB_ROOT_FILEPATH) / "source" / "stocks"
    filepath_target = Path(DATAHUB_ROOT_FILEPATH) / "target" / "stocks"

    # Load source-data
    etf_portfolio = load_data(
        filepath_source / "source_stocks_portfolio_trades.ods",
        file_type="excel",
        sheet_name="Buys",
    )
    etf_portfolio = convert_columns_to_timestamp(
        data=etf_portfolio,
        column_formats={"Datum": "%d.%m.%Y"},
    )
    etf_mergers = load_data(
        filepath_source / "source_stock_mergers.ods",
        file_type="excel",
    )
    etf_mergers = convert_columns_to_timestamp(
        data=etf_mergers,
        column_formats={"date": "%d.%m.%Y"},
    )
    logger.info("Data loaded!")
    etf_isin_valid = get_valid_etf_list(
        etf_data=etf_portfolio,
        etf_mergers=etf_mergers,
        clean=True,
    )

    # Extract: Stocks Datahub
    if init:
        initialize_master_data(
            etf_isins=etf_isin_valid,
            out_path=filepath_source / "source_master_data.csv",
        )
    etf_prices = extract_current_etf_price_data(
        etf_isin_valid,
    )
    #
    # # TRANSFORM: Stocks Datahub
    # transform_etf_master(
    #     df_etf_master,
    #     df_etf_regionMap,
    #     out_path=filepath_etf_master,
    # )
    # transform_historization_etf_prices(
    #     df_etf_prices,
    #     out_path=filepath_etf_prices,
    # )


if __name__ == "__main__":
    run_stocks()
