"""Calculation of dashboard-ready target tables.

The target stage converts transform-stage data into the format consumed
directly by the dashboard, so no calculations are required there anymore.

Cashflow is built at transaction level first: every source (toshl, ETF and
stock trades, dividends, interest, Riester, investment costs) is turned into
rows of [date, tag, category, amount], where `amount` is positive for an inflow
and negative for an outflow. The monthly cashflow tables are then aggregated
from those very same transactions, so both granularities always agree.
"""

import logging
from pathlib import Path

import pandas as pd

from utils.file_io import load_data

logger = logging.getLogger(__name__)

# Schema of every transaction-level cashflow table
TRANSACTION_COLUMNS = ["date", "tag", "category", "amount"]


def _build_transactions(
    dates: pd.Series, tag, category, amounts: pd.Series
) -> pd.DataFrame:
    """Assembles a transaction table from a date and an amount series, dropping
    the rows that move no money (0.0 or missing) so only real bookings remain.

    `tag` and `category` are either a constant or a series aligned with `dates`.
    """
    transactions = pd.DataFrame(
        {
            "date": pd.to_datetime(dates).reset_index(drop=True),
            "tag": tag if isinstance(tag, str) else tag.reset_index(drop=True),
            "category": (
                category
                if isinstance(category, str)
                else category.reset_index(drop=True)
            ),
            "amount": pd.to_numeric(amounts).reset_index(drop=True),
        }
    )
    return transactions[transactions["amount"].fillna(0.0) != 0.0].reset_index(
        drop=True
    )


def map_tags_to_categories(df: pd.DataFrame, tag_category_map: dict) -> pd.DataFrame:
    """
    Maps the toshl tag of every transaction onto its custom category.

    Parameters
    ----------
    df: Cashflow transactions with columns [date, tag, amount]
    tag_category_map: Mapping of custom categories to a list of toshl tags. Needs to be
        category map for either income or expenses, matching the side `df` holds.

    Returns
    -------
    The transactions with an added `category` column, reduced to TRANSACTION_COLUMNS
    """
    tag_to_category = {
        tag: category for category, tags in tag_category_map.items() for tag in tags
    }

    not_categorized = sorted(set(df["tag"]).difference(tag_to_category))
    assert (
        len(not_categorized) == 0
    ), f"There are some tags, which are not yet categorized: {not_categorized}"

    result = df.copy()
    result["category"] = result["tag"].map(tag_to_category)
    return result[TRANSACTION_COLUMNS]


def build_investment_transactions(investment_trades: pd.DataFrame) -> pd.DataFrame:
    """Turns the individual ETF and non-portfolio stock trades into cashflow
    transactions.

    `total_investment` is positive for buys (money spent) and negative for sells
    (proceeds received), so negating it yields the cashflow sign convention
    directly: a buy becomes a negative outflow, a sell a positive inflow. Both
    are booked under the `Investment` category. The order fees (`cost`, positive
    for buys and sells alike) are negated into their own `order_costs`
    transactions, keeping them separate from the invested amount."""
    investments = _build_transactions(
        investment_trades["date"],
        "Investment",
        "Investment",
        -investment_trades["total_investment"],
    )
    order_costs = _build_transactions(
        investment_trades["date"],
        "order_costs",
        "order_costs",
        -investment_trades["cost"],
    )
    return pd.concat([investments, order_costs], ignore_index=True)


def build_rebalancing_transactions(monthly_rebalancing: pd.DataFrame) -> pd.DataFrame:
    """Turns the monthly *net* rebalancing cashflow into one transaction per month.

    Rebalancing sells fund the rebalancing buys, so only the monthly net actually
    leaves (or enters) the account and there is no single trade to book it
    against. The net is therefore kept as one month-end dated row. It is folded
    into the `Investment` category rather than tracked as its own, but keeps
    `Rebalancing` as its tag so it stays distinguishable."""
    net = (
        monthly_rebalancing["income_rebalancing"]
        - monthly_rebalancing["expense_rebalancing"]
    )
    return _build_transactions(
        monthly_rebalancing["date"], "Rebalancing", "Investment", net
    )


def build_dividend_transactions(dividends: pd.DataFrame) -> pd.DataFrame:
    """Turns the received ETF distributions and stock dividends into `Dividends`
    transactions, one per payout. The amounts are received money and therefore
    already positive inflows."""
    return _build_transactions(
        dividends["date"], "Dividends", "Dividends", dividends["dividend"]
    )


def build_interest_transactions(interest: pd.DataFrame) -> pd.DataFrame:
    """Turns the received interest on cash deposits into transactions, one per
    payout, categorised by the deposit kind (`Tagesgeld` / `Festgeld`). The
    amounts are received money and therefore already positive inflows."""
    return _build_transactions(
        interest["date"],
        interest["category"],
        interest["category"],
        interest["interest"],
    )


def build_riester_transactions(riester: pd.DataFrame) -> pd.DataFrame:
    """Turns the Riester pension contributions into `Riester` transactions, one
    per contribution. `amount` is the positive amount paid in and is negated
    into an outflow."""
    return _build_transactions(
        riester["date"], "Riester", "Riester", -riester["amount"]
    )


def load_investment_cost_transactions(filepath: Path) -> pd.DataFrame:
    """Loads the combined investment-cost report (comdirect cost reports +
    Trade Republic fees) as individual ``Investment Costs`` transactions.

    The source file stores each cost in the ``Kosten`` column as a *positive*
    EUR amount (a cost you paid), regardless of provider, so every cost is
    negated into an outflow. A *negative* cost in the file (e.g. the 2020
    EUWAX-Gold credit) therefore becomes a positive inflow and is booked on the
    income side.
    """
    costs = load_data(filepath, sep=",", decimal=".")
    return _build_transactions(
        pd.to_datetime(costs["Datum"], format="%d.%m.%Y"),
        "Investment Costs",
        "Investment Costs",
        -costs["Kosten"],
    )


def aggregate_transactions_monthly(transactions: pd.DataFrame) -> pd.DataFrame:
    """Sums transaction-level cashflow per month and category.

    Returns a long-format table [date, category, amount] where date is the
    month-end (last day of the month)."""
    return transactions.groupby(
        [pd.Grouper(key="date", freq="ME"), "category"], as_index=False
    )["amount"].sum()


def transform_cashflow_to_wide_format(monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Changes the format of the monthly cashflow data from longlist to wide format
    to easily do computations and plots of the cashflow data.

    Parameters
    ----------
    monthly: Monthly cashflow data as longlist with columns [date, category, amount]

    Returns
    -------
    Dataframe in wide format where each column is a category and date is a column
    """
    assert set(monthly.columns) == {
        "date",
        "category",
        "amount",
    }, f"Expected columns [date, category, amount], got {list(monthly.columns)}"

    pivot = (
        monthly.pivot_table(
            index="date", columns="category", values="amount", aggfunc="sum"
        )
        .fillna(0.0)
        .rename_axis(columns=None)
    )

    ### Keep only categories with non-zero total amount in result
    nonzero_categories = [
        category for category in pivot.columns if pivot[category].sum() != 0.0
    ]

    # Get date from index
    return pivot[nonzero_categories].reset_index()
