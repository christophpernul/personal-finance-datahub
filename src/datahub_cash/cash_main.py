import logging

import pandas as pd

from utils.file_io import save_data, load_data
from datahub_cash.transform.balance_sheet import (
    preprocess_accounts,
    preprocess_balances,
    preprocess_term_deposits,
    validate_references,
    calculate_monthly_balances,
    calculate_term_deposit_overview,
    calculate_monthly_liquidity,
)
from constants import (
    PATH_CASH_BALANCE_SHEET,
    CASH_SHEET_ACCOUNTS,
    CASH_SHEET_BALANCES,
    CASH_SHEET_TERM_DEPOSITS,
    PATH_CASH_BALANCES_MONTHLY,
    PATH_CASH_TERM_DEPOSITS,
    PATH_CASH_LIQUIDITY_MONTHLY,
)


logger = logging.getLogger(__name__)


def run_cash(today: pd.Timestamp | None = None) -> None:
    """Runs the cash transform stage: account balances, fixed-term deposits and
    the split of all cash into available and bound money."""
    today = pd.Timestamp.today().normalize() if today is None else today

    ### LOAD & PREPROCESS
    def load(sheet: str) -> pd.DataFrame:
        return load_data(PATH_CASH_BALANCE_SHEET, file_type="excel", sheet_name=sheet)

    accounts = preprocess_accounts(load(CASH_SHEET_ACCOUNTS))
    balances = preprocess_balances(load(CASH_SHEET_BALANCES))
    deposits = preprocess_term_deposits(load(CASH_SHEET_TERM_DEPOSITS))
    validate_references(accounts, balances, deposits)
    logger.info(
        f"Cash balance sheet loaded: {len(accounts)} accounts, {len(balances)} "
        f"balances, {len(deposits)} fixed-term deposits"
    )

    ### TRANSFORM
    monthly_balances = calculate_monthly_balances(accounts, balances, today)
    save_data(monthly_balances, PATH_CASH_BALANCES_MONTHLY)
    stale = monthly_balances[
        (monthly_balances["date"] == monthly_balances["date"].max())
        & monthly_balances["is_stale"]
    ]
    if not stale.empty:
        logger.warning(
            f"Outdated balances (older than the stale limit): "
            f"{stale['account_id'].tolist()}"
        )

    save_data(calculate_term_deposit_overview(deposits, today), PATH_CASH_TERM_DEPOSITS)
    save_data(
        calculate_monthly_liquidity(monthly_balances, deposits, today),
        PATH_CASH_LIQUIDITY_MONTHLY,
    )
    logger.info("Cash balances, fixed-term deposits and liquidity saved!")


if __name__ == "__main__":
    run_cash()
