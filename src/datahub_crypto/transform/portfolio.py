"""Calculates the crypto holdings, their cost basis and the portfolio value from
the ledger (see `preprocessing.LEDGER_COLUMNS`).

The cost basis follows the average-cost method over all exchanges and wallets
combined:

- EUR deposited to an exchange enters at face value, EUR withdrawn leaves.
- A trade moves the average cost of the coins sold onto the coins bought. A
  crypto-to-crypto swap therefore carries over what was originally paid in EUR,
  while a sale for EUR realizes the difference between proceeds and cost.
- Rewards (airdrops) enter at zero cost.
- Removals (coins that left without any known counterpart) take the coins out
  at average cost without booking proceeds or a gain/loss; that cost is kept as
  `removed_cost`.
- Coins moved between own exchanges/wallets keep their cost; only the fee of
  the move is charged.
- A fee consumes coins at their average cost. Its EUR value is booked as
  transaction cost of the coin traded (the coin bought, or the coin sold when
  selling for EUR) or of the coin moved.

So for every coin `total_investment` (cost basis of the coins still held) plus
`total_cost` (fees) plus `removed_cost` is the EUR paid for it.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass

import pandas as pd


logger = logging.getLogger(__name__)

FIAT = "EUR"
# Smaller amounts are rounding leftovers of the exchanges and count as zero
DUST = 1e-9

TRADE_COLUMNS = [
    "date",
    "exchange",
    "refid",
    "coin_sold",
    "amount_sold",
    "coin_bought",
    "amount_bought",
    "cost_basis",
    "fee_eur",
    "realized_gain",
]
COIN_COLUMNS = [
    "date",
    "coin",
    "name",
    "amount",
    "price",
    "value",
    "total_investment",
    "total_cost",
    "value_gained",
    "value_gained_pct",
    "realized_gain",
    "removed_cost",
    "symbol",
    "currency",
]
PORTFOLIO_COLUMNS = [
    "date",
    "exchange",
    "coin",
    "name",
    "amount",
    "price",
    "value",
    "total_investment",
    "total_cost",
    "value_gained",
    "value_gained_pct",
    "symbol",
    "currency",
]


@dataclass
class _Position:
    """Running state of one coin over all exchanges and wallets."""

    amount: float = 0.0
    cost_basis: float = 0.0
    fees: float = 0.0
    realized_gain: float = 0.0
    removed_cost: float = 0.0


def _remove(position: _Position, coin: str, amount: float) -> float:
    """Takes `amount` coins out of the position and returns their cost basis:
    the average cost for coins, the face value for EUR."""
    if coin == FIAT:
        cost = amount
    elif position.amount > DUST:
        cost = position.cost_basis * min(amount / position.amount, 1.0)
    else:
        cost = 0.0
    position.amount -= amount
    position.cost_basis -= cost
    return cost


def _book_transfer(positions: dict, row) -> None:
    """Books a deposit, withdrawal or reward row."""
    position = positions[row.coin]
    if row.coin == FIAT and row.type in ("deposit", "withdrawal"):
        # money moved from or to the bank account
        position.amount += row.amount
        position.cost_basis += row.amount
    elif row.type == "reward":
        position.amount += row.amount
    elif row.type == "removal":
        position.removed_cost += _remove(position, row.coin, -row.amount)
    # Coins moved between own exchanges/wallets: the withdrawal and the deposit
    # leg cancel out, so only the fee changes the position.
    if row.fee:
        position.fees += _remove(position, row.coin, row.fee)


def _book_trade(positions: dict, legs: pd.DataFrame) -> dict:
    """Books all legs of one trade and returns its summary row."""
    exchange, refid = legs["exchange"].iloc[0], legs["refid"].iloc[0]
    per_coin = legs.groupby("coin")["amount"].sum()
    sold, bought = per_coin[per_coin < 0], per_coin[per_coin > 0]
    if len(sold) != 1 or len(bought) != 1:
        raise ValueError(
            f"Trade {refid} on {exchange} must sell and buy exactly one coin, "
            f"got {per_coin.to_dict()}"
        )
    coin_sold, amount_sold = sold.index[0], -sold.iloc[0]
    coin_bought, amount_bought = bought.index[0], bought.iloc[0]

    if positions[coin_sold].amount < amount_sold * (1 - 1e-6):
        logger.warning(
            f"Trade {refid} on {exchange} sells {amount_sold} {coin_sold}, but only "
            f"{positions[coin_sold].amount} are known: an export or manual "
            f"transaction is missing!"
        )

    is_sale = coin_bought == FIAT
    cost_basis = _remove(positions[coin_sold], coin_sold, amount_sold)
    positions[coin_bought].amount += amount_bought
    positions[coin_bought].cost_basis += amount_bought if is_sale else cost_basis
    realized_gain = amount_bought - cost_basis if is_sale else 0.0
    positions[coin_sold].realized_gain += realized_gain

    fee_eur = sum(
        _remove(positions[coin], coin, fee)
        for coin, fee in zip(legs["coin"], legs["fee"])
        if fee
    )
    positions[coin_sold if is_sale else coin_bought].fees += fee_eur

    return {
        "date": legs["date"].iloc[0],
        "exchange": exchange,
        "refid": refid,
        "coin_sold": coin_sold,
        "amount_sold": amount_sold,
        "coin_bought": coin_bought,
        "amount_bought": amount_bought,
        "cost_basis": cost_basis,
        "fee_eur": fee_eur,
        "realized_gain": realized_gain,
    }


def calculate_cost_basis(ledger: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Books the ledger chronologically against one position per coin.

    Parameters
    ----------
    ledger: chronologically sorted ledger with LEDGER_COLUMNS

    Returns
    -------
    trades: one row per trade with TRADE_COLUMNS, where `cost_basis` is the EUR
        cost moved by the trade (the EUR paid when buying with EUR) and
        `fee_eur` the EUR value of its fees
    coins: one row per coin ever held with columns
        [coin, amount, total_investment, total_cost, realized_gain, removed_cost]
    """
    positions: dict[str, _Position] = defaultdict(_Position)
    trades = []

    # All legs of a trade are booked together, every other row on its own
    is_trade = ledger["type"] == "trade"
    booking = (ledger["exchange"] + "|" + ledger["refid"].astype(str)).where(
        is_trade, "row|" + ledger.index.astype(str)
    )
    for _, rows in ledger.groupby(booking, sort=False):
        if rows["type"].iloc[0] == "trade":
            trades.append(_book_trade(positions, rows))
        else:
            _book_transfer(positions, next(rows.itertuples()))

    coins = pd.DataFrame(
        [
            {
                "coin": coin,
                "amount": position.amount,
                "total_investment": position.cost_basis,
                "total_cost": position.fees,
                "realized_gain": position.realized_gain,
                "removed_cost": position.removed_cost,
            }
            for coin, position in positions.items()
        ]
    )
    numeric = [
        "amount",
        "total_investment",
        "total_cost",
        "realized_gain",
        "removed_cost",
    ]
    coins[numeric] = coins[numeric].mask(coins[numeric].abs() < DUST, 0.0)
    return (
        pd.DataFrame(trades, columns=TRADE_COLUMNS),
        coins.sort_values("coin").reset_index(drop=True),
    )


def calculate_current_holdings(ledger: pd.DataFrame) -> pd.DataFrame:
    """Sums the ledger into the current amount of every coin per exchange or
    wallet, columns [exchange, coin, amount]. Empty positions are dropped."""
    holdings = (
        ledger.assign(amount=ledger["amount"] - ledger["fee"])
        .groupby(["exchange", "coin"], as_index=False)["amount"]
        .sum()
    )
    negative = holdings[holdings["amount"] < -DUST]
    if not negative.empty:
        logger.warning(
            f"Negative crypto balances, an export or manual transaction is "
            f"missing:\n{negative.to_string(index=False)}"
        )
    return holdings[holdings["amount"].abs() > DUST].reset_index(drop=True)


def check_holdings(holdings: pd.DataFrame, coins: pd.DataFrame) -> None:
    """Warns about coins whose amount from the cost basis calculation differs
    from the sum over all exchanges. That happens when a coin was moved to or
    from an exchange/wallet without an export, i.e. only one leg of the move is
    known."""
    ledger_amount = holdings.groupby("coin")["amount"].sum()
    comparison = coins.set_index("coin")[["amount"]].join(
        ledger_amount.rename("ledger_amount"), how="outer"
    )
    comparison = comparison.fillna(0.0)
    difference = (comparison["amount"] - comparison["ledger_amount"]).abs()
    mismatches = comparison[difference > 1e-6]
    if not mismatches.empty:
        logger.warning(
            f"Coins moved without a matching deposit/withdrawal (add the missing "
            f"leg to the manual transactions):\n{mismatches.to_string()}"
        )


def aggregate_monthly_holdings(
    ledger: pd.DataFrame, until: pd.Timestamp | None = None
) -> pd.DataFrame:
    """Computes the cumulative amount of every coin per exchange/wallet at each
    month-end, from the first booking until the month of `until` (default:
    today), columns [date, exchange, coin, amount]. Empty positions are
    dropped."""
    monthly = (
        ledger.assign(change=ledger["amount"] - ledger["fee"])
        .groupby([pd.Grouper(key="date", freq="ME"), "exchange", "coin"])["change"]
        .sum()
        .unstack(["exchange", "coin"], fill_value=0.0)
    )
    until = pd.Timestamp.today() if until is None else pd.Timestamp(until)
    last_month = max(monthly.index.max(), until.normalize() + pd.offsets.MonthEnd(0))
    full_range = pd.date_range(monthly.index.min(), last_month, freq="ME")
    monthly = monthly.reindex(full_range, fill_value=0.0)
    monthly.index.name = "date"

    result = monthly.cumsum().melt(ignore_index=False, value_name="amount")
    result = result.reset_index()[["date", "exchange", "coin", "amount"]]
    return result[result["amount"].abs() > DUST].reset_index(drop=True)


def calculate_coin_overview(
    coins: pd.DataFrame,
    holdings: pd.DataFrame,
    prices: pd.DataFrame,
    master_data: pd.DataFrame,
) -> pd.DataFrame:
    """Values every coin ever held with its current EUR price.

    Parameters
    ----------
    coins: cost basis per coin, as returned by `calculate_cost_basis`
    holdings: current amount per exchange/wallet (`calculate_current_holdings`)
    prices: current prices with columns [coin, date, price] in EUR
    master_data: coin master data with columns [coin, name, symbol, currency]

    Returns
    -------
    One row per coin with COIN_COLUMNS. `value_gained` is the unrealized gain
    after fees (value - total_investment - total_cost), `realized_gain` what
    sales for EUR earned above their cost basis. Coins without a price have no
    value.
    """
    overview = coins[
        ["coin", "total_investment", "total_cost", "realized_gain", "removed_cost"]
    ]
    overview = overview.merge(
        holdings.groupby("coin", as_index=False)["amount"].sum(), on="coin", how="left"
    )
    overview["amount"] = overview["amount"].fillna(0.0)
    overview = overview.merge(
        master_data[["coin", "name", "symbol", "currency"]], on="coin", how="left"
    )
    overview = overview.merge(prices[["coin", "price"]], on="coin", how="left")
    overview.loc[overview["coin"] == FIAT, "price"] = 1.0
    overview["date"] = (
        prices["date"].max() if not prices.empty else pd.Timestamp.today().normalize()
    )

    without_master_data = overview.loc[overview["name"].isna(), "coin"].tolist()
    if without_master_data:
        logger.warning(
            f"Coins missing in the crypto master data: {without_master_data}"
        )
    without_price = overview.loc[
        overview["price"].isna() & (overview["amount"] > 0), "coin"
    ].tolist()
    if without_price:
        logger.warning(f"No current price for held coins: {without_price}")

    overview["value"] = overview["amount"] * overview["price"]
    overview = _add_gains(overview)
    return overview[COIN_COLUMNS].sort_values("coin").reset_index(drop=True)


def calculate_crypto_portfolio_value(
    coin_overview: pd.DataFrame, holdings: pd.DataFrame
) -> pd.DataFrame:
    """Splits the coin overview onto the exchanges/wallets holding the coins.

    The cost basis and fees are tracked per coin over all exchanges, so each
    exchange gets the share matching its part of the coin's amount.

    Returns
    -------
    One row per held coin and exchange/wallet with PORTFOLIO_COLUMNS.
    """
    portfolio = holdings.merge(
        coin_overview.drop(columns=["amount", "value", "realized_gain"]),
        on="coin",
        how="left",
    )
    share = portfolio["amount"] / portfolio.groupby("coin")["amount"].transform("sum")
    portfolio["total_investment"] = portfolio["total_investment"] * share
    portfolio["total_cost"] = portfolio["total_cost"] * share
    portfolio["value"] = portfolio["amount"] * portfolio["price"]
    portfolio = _add_gains(portfolio)

    total_value = portfolio["value"].sum()
    invested = portfolio["total_investment"].sum()
    costs = portfolio["total_cost"].sum()
    logger.info(
        f"Crypto portfolio value: {total_value:,.2f} EUR (invested: "
        f"{invested:,.2f} EUR, fees: {costs:,.2f} EUR, gained: "
        f"{total_value - invested - costs:,.2f} EUR)"
    )
    return (
        portfolio[PORTFOLIO_COLUMNS]
        .sort_values(["exchange", "value"], ascending=[True, False])
        .reset_index(drop=True)
    )


def _add_gains(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the unrealized gain after fees, absolute and relative to the cost
    basis (undefined for coins received for free)."""
    df = df.copy()
    df["value_gained"] = df["value"] - df["total_investment"] - df["total_cost"]
    invested = df["total_investment"].where(df["total_investment"].abs() > DUST)
    df["value_gained_pct"] = df["value_gained"] / invested * 100
    return df
