import logging

from datahub_cashflow.cashflow_main import run_cashflow

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

run_cashflow()
disable_existing_loggers = False
