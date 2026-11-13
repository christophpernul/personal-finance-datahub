"""Sold (closed) ETF and stock positions with their realised profit and return.

A *position* is the life of one security in the portfolio: it opens with the
first buy while nothing of it is held and closes with the sell that takes the
holding back to (practically) zero. A security bought again later starts a new
position, so a sell-and-rebuy (e.g. to use the tax-free allowance) closes one
position and opens the next one.

Every closed position is valued from the investor's view with all of its
cashflows:

- buys (money paid in) and sells (proceeds received),
- order costs of every buy and sell,
- dividends received while the position was open,
- ongoing costs (the yearly per-security cost report, e.g. comdirect
  Wertpapierkosten), split over the positions of the security held in that year.

`net_profit` = sell proceeds + dividends - buy amount - all costs, and
`roi_pct` relates it to the buy amount. `annual_return_pct` is the
money-weighted return per year (XIRR) of the same flows, so it accounts for how
long each euro was invested (e.g. monthly savings plans).
"""

import logging
import math

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# A sell closes a position when what is left of it is at most this share of
# the most shares held in it: fractional ETF shares derived from amount / price
# never sum to exactly zero (rounding of the quoted prices).
CLOSE_TOLERANCE = 0.01

# Flow types of the per-position flow table (investor's view: money paid in is
# negative, money received positive)
FLOW_BUY = "Buy"
FLOW_SELL = "Sell"
FLOW_ORDER_COSTS = "Order costs"
FLOW_DIVIDEND = "Dividend"
FLOW_ONGOING_COSTS = "Ongoing costs"

SOLD_POSITION_COLUMNS = [
    "position_id",
    "isin",
    "name",
    "security_type",
    "depot",
    "open_date",
    "close_date",
    "holding_days",
    "shares_bought",
    "buy_amount",
    "sell_amount",
    "price_gain",
    "dividends",
    "order_costs",
    "ongoing_costs",
    "total_costs",
    "net_profit",
    "roi_pct",
    "annual_return_pct",
    "rebought",
]
FLOW_COLUMNS = ["position_id", "date", "type", "amount"]


def assign_positions(
    trades: pd.DataFrame, tolerance: float = CLOSE_TOLERANCE
) -> pd.DataFrame:
    """Numbers the positions of every security and tags each trade with the
    position it belongs to.

    Parameters
    ----------
    trades: preprocessed trades [date, isin, shares, trade_type, ...], where
        `shares` is positive for buys and negative for sells (after mergers and
        splits, so share counts are comparable over time)
    tolerance: a sell closes the position when at most this share of the most
        shares held in it is left

    Returns the trades with `position_no` (1, 2, ... per isin), `position_id`
    (``<isin>#<position_no>``) and `closes_position` (True on the closing sell).
    Trades of one day are taken sells first: when a day both sells and buys a
    security, the sells end the old position and the buys open a new one.
    """
    trades = trades.copy()
    trades["_buy_last"] = trades["trade_type"] == "buy"
    trades = trades.sort_values(["isin", "date", "_buy_last"], kind="stable").drop(
        columns="_buy_last"
    )

    position_no = np.zeros(len(trades), dtype=int)
    closes = np.zeros(len(trades), dtype=bool)
    shares = trades["shares"].to_numpy(dtype=float)
    is_sell = (trades["trade_type"] == "sell").to_numpy()
    for rows in trades.groupby("isin", sort=False).indices.values():
        number, held, peak = 1, 0.0, 0.0
        closing_row = None  # closing sell of the last position, until a buy
        for i in rows:
            if is_sell[i] and closing_row is not None:
                # The rest of a sale spread over several orders: it still
                # belongs to (and now closes) the position just closed.
                position_no[i] = number - 1
                closes[closing_row], closes[i] = False, True
                closing_row = i
                continue
            position_no[i] = number
            held += shares[i]
            peak = max(peak, held)
            closing_row = None
            if is_sell[i] and peak > 0 and held <= tolerance * peak:
                closes[i] = True
                closing_row = i
                number, held, peak = number + 1, 0.0, 0.0

    trades["position_no"] = position_no
    trades["position_id"] = trades["isin"] + "#" + trades["position_no"].astype(str)
    trades["closes_position"] = closes
    return trades.reset_index(drop=True)


def _position_periods(
    trades: pd.DataFrame, valuation_date: pd.Timestamp
) -> pd.DataFrame:
    """[position_id, isin, open_date, close_date, end_date, closed] per position,
    where `end_date` is the close date or `valuation_date` for open positions."""
    periods = trades.groupby("position_id").agg(
        isin=("isin", "first"),
        open_date=("date", "min"),
        last_date=("date", "max"),
        closed=("closes_position", "any"),
    )
    periods["close_date"] = periods["last_date"].where(periods["closed"])
    periods["end_date"] = periods["close_date"].fillna(valuation_date)
    return periods.drop(columns="last_date").reset_index()


def allocate_dividends(
    dividends: pd.DataFrame, dividend_keys: pd.Series, periods: pd.DataFrame
) -> pd.DataFrame:
    """Assigns every dividend to the position of its security that was open on
    the payment date. A dividend paid after a position closed and before the
    next one opened (the payment trails the ex-date) goes to the position
    closed last. Returns [position_id, date, amount]."""
    divs = pd.DataFrame(
        {
            "isin": dividend_keys.to_numpy(),
            "date": dividends["date"].to_numpy(),
            "amount": dividends["dividend"].to_numpy(),
        }
    ).dropna(subset=["amount"])
    return _assign_to_latest_open(divs, periods)


def _assign_to_latest_open(rows: pd.DataFrame, periods: pd.DataFrame) -> pd.DataFrame:
    """Attaches `position_id` of the latest position of the same isin opened on
    or before each row's date; rows before any position are dropped (logged)."""
    rows = rows.sort_values("date")
    opens = periods[["isin", "open_date", "position_id"]].sort_values("open_date")
    matched = pd.merge_asof(
        rows,
        opens,
        left_on="date",
        right_on="open_date",
        by="isin",
        direction="backward",
    )
    unmatched = matched["position_id"].isna()
    if unmatched.any():
        logger.warning(
            "Rows without a position of their security are left out: "
            f"{sorted(matched.loc[unmatched, 'isin'].unique())}"
        )
    return matched.loc[~unmatched, ["position_id", "date", "amount"]]


def allocate_ongoing_costs(costs: pd.DataFrame, periods: pd.DataFrame) -> pd.DataFrame:
    """Splits the yearly per-security costs over the positions of the security.

    Parameters
    ----------
    costs: [date, isin, cost] with `cost` positive for a cost paid (a negative
        cost is a credit); rows without an ISIN are depot-wide costs that belong
        to no single position and are left out
    periods: positions as returned by `_position_periods`

    A cost report covers the calendar year of its date, so a cost is split over
    the positions of its security by the number of days each of them was held
    in that year. A cost for a year in which no position was held goes to the
    latest position opened before it. Returns [position_id, date, amount] with
    `amount` negative for a cost paid.
    """
    costs = costs.dropna(subset=["isin", "cost"])
    costs = costs[costs["isin"] != ""]
    allocated = []
    unplaced = []
    for _, cost in costs.iterrows():
        year_start = pd.Timestamp(year=cost["date"].year, month=1, day=1)
        year_end = pd.Timestamp(year=cost["date"].year, month=12, day=31)
        own = periods[periods["isin"] == cost["isin"]]
        overlap_start = own["open_date"].clip(lower=year_start)
        overlap_end = own["end_date"].clip(upper=year_end)
        days = ((overlap_end - overlap_start).dt.days + 1).clip(lower=0)
        if days.sum() > 0:
            share = days / days.sum()
            allocated.append(
                pd.DataFrame(
                    {
                        "position_id": own["position_id"],
                        "date": cost["date"],
                        "amount": -cost["cost"] * share,
                    }
                )
            )
        else:
            unplaced.append(cost)
    if unplaced:
        unplaced = pd.DataFrame(unplaced).assign(amount=lambda df: -df["cost"])
        allocated.append(_assign_to_latest_open(unplaced, periods))
    if not allocated:
        return pd.DataFrame(columns=["position_id", "date", "amount"])
    result = pd.concat(allocated, ignore_index=True)
    return result[result["amount"] != 0.0]


def xirr(dates: pd.Series, amounts: pd.Series) -> float:
    """Yearly rate (in %) at which the flows discount to zero; NaN unless the
    flows both pay in and pay out."""
    amounts = np.asarray(amounts, dtype=float)
    if not ((amounts < 0).any() and (amounts > 0).any()):
        return math.nan
    dates = pd.to_datetime(pd.Series(dates))
    years = ((dates - dates.min()).dt.days / 365.25).to_numpy()
    if years.max() == 0:
        return math.nan

    def npv(rate: float) -> float:
        return float((amounts / (1.0 + rate) ** years).sum())

    low, high = -0.9999, 1.0
    while npv(high) > 0 and high < 1e6:
        high *= 2
    if npv(low) * npv(high) > 0:
        return math.nan
    for _ in range(200):
        mid = (low + high) / 2
        if npv(low) * npv(mid) <= 0:
            high = mid
        else:
            low = mid
    return (low + high) / 2 * 100


def build_position_flows(
    trades: pd.DataFrame, dividends: pd.DataFrame, ongoing_costs: pd.DataFrame
) -> pd.DataFrame:
    """Every cashflow of every position from the investor's view
    [position_id, date, type, amount]: buys and costs negative, sells and
    dividends positive, so the flows of a position sum to its net profit.

    `trades` holds `total_investment` (+spent on buys, -proceeds on sells) and
    `cost` (order costs). Order costs count as paid whatever their sign in the
    source sheets."""
    is_buy = trades["trade_type"] == "buy"
    trade_flows = pd.DataFrame(
        {
            "position_id": trades["position_id"],
            "date": trades["date"],
            "type": np.where(is_buy, FLOW_BUY, FLOW_SELL),
            "amount": -trades["total_investment"],
        }
    )
    order_costs = pd.DataFrame(
        {
            "position_id": trades["position_id"],
            "date": trades["date"],
            "type": FLOW_ORDER_COSTS,
            "amount": -trades["cost"].fillna(0.0).abs(),
        }
    )
    flows = pd.concat(
        [
            trade_flows,
            order_costs,
            dividends.assign(type=FLOW_DIVIDEND),
            ongoing_costs.assign(type=FLOW_ONGOING_COSTS),
        ],
        ignore_index=True,
    )
    flows["amount"] = pd.to_numeric(flows["amount"])
    flows = flows[flows["amount"].fillna(0.0) != 0.0]
    return flows.sort_values(["position_id", "date"], kind="stable").reset_index(
        drop=True
    )[FLOW_COLUMNS]


def build_sold_positions(
    trades: pd.DataFrame,
    dividends: pd.DataFrame,
    dividend_keys: pd.Series,
    costs: pd.DataFrame,
    valuation_date: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Builds the sold positions and their single cashflows.

    Parameters
    ----------
    trades: every ETF and stock trade (incl. rebalancing, after mergers and
        splits) [date, isin, name, total_investment, cost, shares, trade_type,
        depot, security_type]
    dividends: preprocessed dividends [date, isin, name, dividend]
    dividend_keys: the trades' `isin` each dividend belongs to, aligned with
        `dividends` (see `dividend_security_keys`)
    costs: ongoing per-security costs [date, isin, cost], `isin` already moved
        to the successor of a merged ETF
    valuation_date: end date of positions still held, for splitting the costs
        of the running year

    Returns (sold_positions, flows): one row per closed position (columns
    `SOLD_POSITION_COLUMNS`) and every cashflow of those positions (columns
    `FLOW_COLUMNS`), dates as YYYY-MM-DD.
    """
    trades = assign_positions(trades)
    periods = _position_periods(trades, valuation_date)
    position_dividends = allocate_dividends(dividends, dividend_keys, periods)
    position_costs = allocate_ongoing_costs(costs, periods)

    closed_ids = set(periods.loc[periods["closed"], "position_id"])
    flows = build_position_flows(trades, position_dividends, position_costs)
    flows = flows[flows["position_id"].isin(closed_ids)]
    closed_trades = trades[trades["position_id"].isin(closed_ids)]
    if closed_trades.empty:
        return (
            pd.DataFrame(columns=SOLD_POSITION_COLUMNS),
            pd.DataFrame(columns=FLOW_COLUMNS),
        )

    def flow_sum(flow_type: str) -> pd.Series:
        return flows[flows["type"] == flow_type].groupby("position_id")["amount"].sum()

    is_buy = closed_trades["trade_type"] == "buy"
    positions = closed_trades.groupby("position_id").agg(
        isin=("isin", "first"),
        # the latest name (e.g. after a renaming merger)
        name=("name", "last"),
        security_type=("security_type", "first"),
        position_no=("position_no", "first"),
    )
    positions["depot"] = closed_trades.groupby("position_id")["depot"].agg(
        lambda depots: ", ".join(sorted(set(depots.dropna())))
    )
    positions = positions.join(
        periods.set_index("position_id")[["open_date", "close_date"]]
    )
    positions["holding_days"] = (
        positions["close_date"] - positions["open_date"]
    ).dt.days
    positions["shares_bought"] = (
        closed_trades[is_buy].groupby("position_id")["shares"].sum()
    )
    positions["buy_amount"] = -flow_sum(FLOW_BUY)
    positions["sell_amount"] = flow_sum(FLOW_SELL)
    positions["dividends"] = flow_sum(FLOW_DIVIDEND)
    positions["order_costs"] = -flow_sum(FLOW_ORDER_COSTS)
    positions["ongoing_costs"] = -flow_sum(FLOW_ONGOING_COSTS)
    money_cols = [
        "shares_bought",
        "buy_amount",
        "sell_amount",
        "dividends",
        "order_costs",
        "ongoing_costs",
    ]
    positions[money_cols] = positions[money_cols].fillna(0.0)
    positions["price_gain"] = positions["sell_amount"] - positions["buy_amount"]
    positions["total_costs"] = positions["order_costs"] + positions["ongoing_costs"]
    positions["net_profit"] = flows.groupby("position_id")["amount"].sum()
    positions["roi_pct"] = (
        positions["net_profit"] / positions["buy_amount"].where(lambda b: b > 0)
    ) * 100
    positions["annual_return_pct"] = flows.groupby("position_id")[
        ["date", "amount"]
    ].apply(lambda f: xirr(f["date"], f["amount"]))
    last_position_no = (
        periods.assign(
            no=periods["position_id"].str.rsplit("#", n=1).str[-1].astype(int)
        )
        .groupby("isin")["no"]
        .max()
    )
    positions["rebought"] = positions["position_no"] < positions["isin"].map(
        last_position_no
    )

    positions = positions.reset_index().sort_values(
        ["close_date", "name"], ascending=[False, True]
    )
    for frame, col in (
        (positions, "open_date"),
        (positions, "close_date"),
        (flows, "date"),
    ):
        frame[col] = pd.to_datetime(frame[col]).dt.strftime("%Y-%m-%d")
    total = positions["net_profit"].sum()
    logger.info(
        f"{len(positions)} sold positions with a net profit of {total:,.2f} EUR"
    )
    return (
        positions[SOLD_POSITION_COLUMNS].reset_index(drop=True),
        flows.reset_index(drop=True),
    )
