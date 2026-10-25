"""Parses the crypto exchange exports (Kraken, Binance, KuCoin) and the manually
maintained transactions into one ledger.

Every ledger row is a single balance change of one coin on one exchange or
wallet, with the columns LEDGER_COLUMNS:

- ``date``: UTC timestamp
- ``exchange``: exchange or wallet holding the coin (e.g. ``kraken``)
- ``refid``: reference shared by all legs of one trade
- ``type``: one of LEDGER_TYPES
- ``coin``: normalized ticker (e.g. ``BTC``, ``EUR``)
- ``amount``: signed amount moved (negative for the coin sold / withdrawn)
- ``fee``: fee charged in ``coin`` on top, so the balance changes by
  ``amount - fee``

A trade has one leg for the coin sold (amount < 0), one for the coin bought
(amount > 0) and optionally fee-only legs (amount 0) for fees paid in a third
coin, e.g. BNB on Binance.
"""

import logging

import pandas as pd


logger = logging.getLogger(__name__)

LEDGER_COLUMNS = ["date", "exchange", "refid", "type", "coin", "amount", "fee"]
# - deposit / withdrawal: EUR moved from / to the bank, or a coin moved between
#   own exchanges and wallets
# - trade: one leg of a trade
# - reward: coins received for free, e.g. airdrops
# - removal: coins known to have left an exchange/wallet without any known
#   counterpart (no export, unknown proceeds), only used in manual transactions
LEDGER_TYPES = {"deposit", "withdrawal", "trade", "reward", "removal"}

# Kraken prefixes its legacy asset codes with X (crypto) and Z (fiat)
KRAKEN_COIN_MAP = {
    "XXBT": "BTC",
    "XBT": "BTC",
    "XETH": "ETH",
    "XXRP": "XRP",
    "XETC": "ETC",
    "XXMR": "XMR",
    "XLTC": "LTC",
    "XXLM": "XLM",
    "XZEC": "ZEC",
    "XXDG": "DOGE",
    "XREP": "REP",
    "XMLN": "MLN",
    "ZEUR": "EUR",
    "ZUSD": "USD",
}
# `spend`/`receive` are the two legs of an instant "Buy Crypto" order;
# `transfer` credits airdropped coins (directly or via the futures wallet), but
# is also used for both sides of the conversion of a delisted coin, which are
# turned into trades via `apply_conversions`.
# Unknown types fail loudly, so a new kind of booking is never silently dropped.
KRAKEN_TYPE_MAP = {
    "deposit": "deposit",
    "withdrawal": "withdrawal",
    "trade": "trade",
    "spend": "trade",
    "receive": "trade",
    "transfer": "reward",
}
KRAKEN_REQUIRED_COLUMNS = {"txid", "refid", "time", "type", "asset", "amount", "fee"}
BINANCE_TRADES_REQUIRED_COLUMNS = {
    "Date(UTC)",
    "Pair",
    "Side",
    "Executed",
    "Amount",
    "Fee",
}
BINANCE_DEPOSITS_REQUIRED_COLUMNS = {
    "Date(UTC)",
    "Coin",
    "Amount",
    "TransactionFee",
    "TXID",
    "Status",
}
KUCOIN_TRADES_REQUIRED_COLUMNS = {
    "oid",
    "symbol",
    "dealValue",
    "amount",
    "fee",
    "direction",
    "createdDate",
}
KUCOIN_DEPOSITS_REQUIRED_COLUMNS = {"Time", "Coin", "Amount"}
MANUAL_REQUIRED_COLUMNS = {"date", "exchange", "type", "coin", "amount", "fee"}

# KuCoin exports its timestamps in Beijing time (UTC+8): deposits on KuCoin
# appear 8 hours "after" the matching withdrawals left another exchange (UTC).
KUCOIN_UTC_OFFSET = pd.Timedelta(hours=8)


def _check_columns(df: pd.DataFrame, required: set, source: str) -> None:
    missing = required.difference(df.columns)
    assert not missing, f"Columns missing in {source} export: {sorted(missing)}"


def combine_export_files(exports: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenates the exports of one kind (e.g. all Binance trade histories).

    Exports covering overlapping periods repeat the same rows, so a row that
    already appeared in an earlier file is dropped. Identical rows within one
    file are kept: those are separate fills of the same size in the same
    second, not duplicates.
    """
    seen_rows: set[str] = set()
    kept = []
    for export in exports:
        row_keys = export.astype(str).apply("|".join, axis=1)
        kept.append(export[~row_keys.isin(seen_rows)])
        seen_rows.update(row_keys)
    if not kept:
        return pd.DataFrame()
    return pd.concat(kept, ignore_index=True)


def _ledger_rows(
    date, exchange: str, refid, row_type, coin, amount, fee=0.0
) -> pd.DataFrame:
    """Assembles ledger rows; every argument is a constant or an aligned series."""
    rows = pd.DataFrame(
        {
            "date": date,
            "exchange": exchange,
            "refid": refid,
            "type": row_type,
            "coin": coin,
            "amount": amount,
            "fee": fee,
        }
    )
    return rows[LEDGER_COLUMNS].reset_index(drop=True)


def _trade_legs(
    date: pd.Series,
    exchange: str,
    refid: pd.Series,
    coin_sold: pd.Series,
    amount_sold: pd.Series,
    coin_bought: pd.Series,
    amount_bought: pd.Series,
    fee_coin: pd.Series,
    fee: pd.Series,
) -> pd.DataFrame:
    """Splits trades given as one row each into their ledger legs: the coin
    sold, the coin bought and a fee-only leg in the coin the fee was paid in."""
    return pd.concat(
        [
            _ledger_rows(date, exchange, refid, "trade", coin_sold, -amount_sold),
            _ledger_rows(date, exchange, refid, "trade", coin_bought, amount_bought),
            _ledger_rows(date, exchange, refid, "trade", fee_coin, 0.0, fee),
        ],
        ignore_index=True,
    )


def _split_amount_and_coin(values: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Splits Binance values like ``1,234.5000000000ABC`` into the amount and
    the coin."""
    parts = values.astype(str).str.strip().str.extract(r"^([\d,]*\.?\d+)([A-Z].*)$")
    unparsed = values[parts.isna().any(axis=1)]
    assert unparsed.empty, f"Cannot split amount and coin of: {list(unparsed)}"
    return parts[0].str.replace(",", "").astype("float64"), parts[1]


def preprocess_kraken_ledgers(ledgers: pd.DataFrame) -> pd.DataFrame:
    """Converts the Kraken ledger export into ledger rows.

    Kraken lists deposits and withdrawals twice: once when initiated (without
    txid) and once when booked (with txid). Only the booked rows change the
    balance, so the others are dropped; overlapping exports are deduplicated
    via the txid.
    """
    _check_columns(ledgers, KRAKEN_REQUIRED_COLUMNS, "Kraken ledger")
    booked = ledgers.dropna(subset=["txid"]).drop_duplicates(subset="txid")

    unknown_types = sorted(set(booked["type"]).difference(KRAKEN_TYPE_MAP))
    if unknown_types:
        raise ValueError(
            f"Unknown Kraken ledger types {unknown_types}, add them to KRAKEN_TYPE_MAP!"
        )

    return _ledger_rows(
        pd.to_datetime(booked["time"]),
        "kraken",
        booked["refid"],
        booked["type"].map(KRAKEN_TYPE_MAP),
        booked["asset"].replace(KRAKEN_COIN_MAP),
        booked["amount"].astype("float64"),
        booked["fee"].astype("float64"),
    )


def preprocess_binance_trades(trades: pd.DataFrame) -> pd.DataFrame:
    """Converts the Binance trade history (one row per fill) into ledger rows.

    `Executed` holds the base coin of the pair and `Amount` the quote coin, so
    a BUY sells the quote coin for the base coin and a SELL the other way
    around. The fee is charged separately in the coin given in `Fee`.
    """
    _check_columns(trades, BINANCE_TRADES_REQUIRED_COLUMNS, "Binance trade history")
    date = pd.to_datetime(trades["Date(UTC)"])
    base_amount, base_coin = _split_amount_and_coin(trades["Executed"])
    quote_amount, quote_coin = _split_amount_and_coin(trades["Amount"])
    fee, fee_coin = _split_amount_and_coin(trades["Fee"])
    is_buy = trades["Side"].str.upper() == "BUY"
    # The export has no trade id; the row number keeps same-second fills apart
    row_number = pd.Series(trades.index.astype(str), index=trades.index)
    refid = "binance-" + date.dt.strftime("%Y%m%d%H%M%S") + "-" + row_number

    return _trade_legs(
        date,
        "binance",
        refid,
        coin_sold=quote_coin.where(is_buy, base_coin),
        amount_sold=quote_amount.where(is_buy, base_amount),
        coin_bought=base_coin.where(is_buy, quote_coin),
        amount_bought=base_amount.where(is_buy, quote_amount),
        fee_coin=fee_coin,
        fee=fee,
    )


def preprocess_binance_deposits(deposits: pd.DataFrame) -> pd.DataFrame:
    """Converts the completed Binance deposits into ledger rows."""
    _check_columns(deposits, BINANCE_DEPOSITS_REQUIRED_COLUMNS, "Binance deposit")
    completed = deposits[deposits["Status"] == "Completed"]
    return _ledger_rows(
        pd.to_datetime(completed["Date(UTC)"]),
        "binance",
        completed["TXID"],
        "deposit",
        completed["Coin"],
        completed["Amount"].astype("float64"),
        completed["TransactionFee"].astype("float64"),
    )


def preprocess_kucoin_trades(trades: pd.DataFrame) -> pd.DataFrame:
    """Converts the KuCoin trade export into ledger rows.

    `symbol` is ``BASE-QUOTE``, `amount` the base and `dealValue` the quote
    amount. KuCoin deducts the fee from the coin received.
    """
    _check_columns(trades, KUCOIN_TRADES_REQUIRED_COLUMNS, "KuCoin trade")
    date = pd.to_datetime(trades["createdDate"]) - KUCOIN_UTC_OFFSET
    base_coin = trades["symbol"].str.split("-").str[0]
    quote_coin = trades["symbol"].str.split("-").str[1]
    is_buy = trades["direction"].str.upper() == "BUY"
    coin_bought = base_coin.where(is_buy, quote_coin)

    return _trade_legs(
        date,
        "kucoin",
        "kucoin-" + trades["oid"].astype(str),
        coin_sold=quote_coin.where(is_buy, base_coin),
        amount_sold=trades["dealValue"].where(is_buy, trades["amount"]),
        coin_bought=coin_bought,
        amount_bought=trades["amount"].where(is_buy, trades["dealValue"]),
        fee_coin=coin_bought,
        fee=trades["fee"],
    )


def preprocess_kucoin_deposits(deposits: pd.DataFrame) -> pd.DataFrame:
    """Converts the KuCoin deposit export into ledger rows."""
    _check_columns(deposits, KUCOIN_DEPOSITS_REQUIRED_COLUMNS, "KuCoin deposit")
    date = pd.to_datetime(deposits["Time"]) - KUCOIN_UTC_OFFSET
    return _ledger_rows(
        date,
        "kucoin",
        "kucoin-deposit-" + date.dt.strftime("%Y%m%d%H%M%S"),
        "deposit",
        deposits["Coin"],
        deposits["Amount"].astype("float64"),
    )


def preprocess_manual_transactions(manual: pd.DataFrame) -> pd.DataFrame:
    """Converts the manually maintained coin movements into ledger rows. They
    fill gaps of the exports, e.g. a withdrawal from an exchange without
    withdrawal export or deposits to a private wallet that has no export at all.

    The legs of a manual trade share the same value in the optional `group`
    column; rows without a group are booked on their own."""
    _check_columns(manual, MANUAL_REQUIRED_COLUMNS, "manual crypto transactions")
    unknown_types = sorted(set(manual["type"]).difference(LEDGER_TYPES))
    assert not unknown_types, f"Unknown manual transaction types: {unknown_types}"
    row_refid = "manual-" + pd.Series(manual.index.astype(str), index=manual.index)
    if "group" in manual.columns:
        refid = ("manual-" + manual["group"].astype("string")).fillna(row_refid)
    else:
        refid = row_refid
    return _ledger_rows(
        pd.to_datetime(manual["date"]),
        manual["exchange"],
        refid,
        manual["type"],
        manual["coin"],
        manual["amount"].astype("float64"),
        manual["fee"].fillna(0.0).astype("float64"),
    )


def combine_ledgers(ledgers: list[pd.DataFrame]) -> pd.DataFrame:
    """Combines the ledgers of all exchanges into one, sorted chronologically
    (stable, so the legs of a trade stay together)."""
    ledger = pd.concat(ledgers, ignore_index=True)
    unknown_types = sorted(set(ledger["type"]).difference(LEDGER_TYPES))
    assert not unknown_types, f"Unknown ledger types: {unknown_types}"
    return ledger.sort_values("date", kind="stable").reset_index(drop=True)


def apply_conversions(ledger: pd.DataFrame, conversions: pd.DataFrame) -> pd.DataFrame:
    """Books pairs of exported rows as the two legs of one trade.

    When an exchange delists a coin it converts the balance into another coin,
    but may book the coin taken and the coin given as two unrelated `transfer`s
    with different refids and dates. Without pairing, the taken coin would
    vanish with its cost basis and the given coin would come in for free.

    Parameters
    ----------
    ledger: ledger with LEDGER_COLUMNS
    conversions: one row per exported booking with columns
        [exchange, refid, group]; both sides of a conversion share a group

    Returns
    -------
    The ledger with both rows of each conversion turned into trade legs sharing
    one refid
    """
    ledger = ledger.copy()
    for group, sides in conversions.groupby("group"):
        is_pair = pd.Series(False, index=ledger.index)
        for side in sides.itertuples():
            is_pair |= (ledger["exchange"] == side.exchange) & (
                ledger["refid"] == side.refid
            )
        legs = ledger[is_pair]
        assert len(legs) == 2 and (legs["amount"] < 0).sum() == 1, (
            f"Conversion {group} needs one negative and one positive exported "
            f"booking, found:\n{legs.to_string()}"
        )
        ledger.loc[is_pair, "type"] = "trade"
        ledger.loc[is_pair, "refid"] = f"conversion-{group}"
    return ledger


def extract_fiat_transfers(ledger: pd.DataFrame, fiat: str = "EUR") -> pd.DataFrame:
    """Returns the money moved between the bank and the exchanges: deposits
    (positive) and withdrawals (negative) of `fiat`, columns
    [date, exchange, amount].

    The withdrawal `fee` is charged on the exchange balance, so `amount` is
    exactly what left or reached the bank account.
    """
    is_transfer = (ledger["coin"] == fiat) & ledger["type"].isin(
        ["deposit", "withdrawal"]
    )
    return ledger.loc[is_transfer, ["date", "exchange", "amount"]].reset_index(
        drop=True
    )
