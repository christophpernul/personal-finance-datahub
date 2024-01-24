import logging
from pathlib import Path

from utils.file_io import get_config_file, save_data, load_data
from constants import DATAHUB_ROOT_FILEPATH


logger = logging.getLogger(__name__)


def run_stocks():
    filepath_source = Path(DATAHUB_ROOT_FILEPATH) / "source" / "stocks"
    filepath_target = Path(DATAHUB_ROOT_FILEPATH) / "target" / "stocks"

    # Load source-data
    df_etf_portfolio = load_data(
        filepath_source / "source_stocks_portfolio_trades.ods",
        file_type="excel",
        sheet_name="Buys",
    )
    logger.info("Data loaded!")
    list_etf_isin = list(
        load_data(filepath_source / "source_stocks_etf_master.ods", file_type="excel")[
            "ISIN"
        ]
        .dropna()
        .drop_duplicates()
    )
    list_etf_isin_valid = list(df_etf_portfolio["ISIN"].dropna().drop_duplicates())

    # Extract: Stocks Datahub
    df_etf_master = extract_etf_master_data(
        list_etf_isin,
        source_url=ETF_SOURCE_URL,
    )
    # df_etf_prices = extract_etf_price_data(
    #     list_etf_isin_valid,
    #     source_url=ETF_SOURCE_URL,
    # )
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
