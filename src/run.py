import logging

from datahub_cashflow.cashflow_main import run_cashflow
from datahub_stocks.stocks_main import run_stocks
from datahub_target.target_main import run_target

console_handler = logging.StreamHandler()
console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("datahub.log"), console_handler],
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


cashflow_incomes, cashflow_expenses = run_cashflow()

(
    portfolio_value,
    monthly_investments,
    monthly_dividends,
    monthly_rebalancing,
    monthly_interest,
) = run_stocks()
run_target(
    cashflow_incomes,
    cashflow_expenses,
    monthly_investments,
    monthly_dividends,
    monthly_rebalancing,
    monthly_interest,
)
disable_existing_loggers = False
