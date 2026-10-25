import pandas as pd
import pytest

from src.datahub_crypto.transform.preprocessing import (
    LEDGER_COLUMNS,
    combine_export_files,
    preprocess_kraken_ledgers,
    preprocess_binance_trades,
    preprocess_kucoin_trades,
    preprocess_kucoin_deposits,
    preprocess_manual_transactions,
    apply_conversions,
    extract_fiat_transfers,
)


def _balances(ledger: pd.DataFrame) -> dict:
    return (
        (ledger["amount"] - ledger["fee"]).groupby(ledger["coin"]).sum().round(10)
    ).to_dict()


@pytest.fixture
def kraken_ledgers():
    # A deposit is exported twice (initiated without txid, booked with txid),
    # followed by a EUR -> BTC trade with the fee charged in EUR.
    return pd.DataFrame(
        {
            "txid": [None, "L1", "L2", "L3"],
            "refid": ["D1", "D1", "T1", "T1"],
            "time": [
                "2020-01-01 10:00:00",
                "2020-01-01 10:05:00",
                "2020-01-02 12:00:00",
                "2020-01-02 12:00:00",
            ],
            "type": ["deposit", "deposit", "trade", "trade"],
            "asset": ["ZEUR", "ZEUR", "ZEUR", "XXBT"],
            "amount": [100.0, 100.0, -50.0, 0.01],
            "fee": [0.0, 0.0, 0.5, 0.0],
        }
    )


@pytest.mark.ut
def test_preprocess_kraken_ledgers_books_deposit_once(kraken_ledgers):
    ledger = preprocess_kraken_ledgers(kraken_ledgers)
    assert list(ledger.columns) == LEDGER_COLUMNS
    assert len(ledger) == 3
    assert _balances(ledger) == {"EUR": 49.5, "BTC": 0.01}
    # both legs of the trade share the refid
    assert ledger.loc[ledger["type"] == "trade", "refid"].tolist() == ["T1", "T1"]


@pytest.mark.ut
def test_preprocess_kraken_ledgers_maps_transfers_to_rewards(kraken_ledgers):
    airdrop = pd.DataFrame(
        {
            "txid": ["L4"],
            "refid": ["A1"],
            "time": ["2020-02-01 00:00:00"],
            "type": ["transfer"],
            "asset": ["ABC"],
            "amount": [10.0],
            "fee": [0.0],
        }
    )
    ledger = preprocess_kraken_ledgers(pd.concat([kraken_ledgers, airdrop]))
    assert ledger.loc[ledger["coin"] == "ABC", "type"].tolist() == ["reward"]


@pytest.mark.ut
def test_preprocess_kraken_ledgers_rejects_unknown_types(kraken_ledgers):
    kraken_ledgers.loc[3, "type"] = "staking"
    with pytest.raises(ValueError, match="staking"):
        preprocess_kraken_ledgers(kraken_ledgers)


@pytest.mark.ut
def test_preprocess_kraken_ledgers_reads_new_export_format():
    # Newer Kraken exports carry plain asset names and additional columns
    ledgers = pd.DataFrame(
        {
            "txid": ["L1", "L2"],
            "refid": ["T1", "T1"],
            "time": ["2024-01-01 10:00:00", "2024-01-01 10:00:00"],
            "type": ["spend", "receive"],
            "subtype": ["", ""],
            "aclass": ["currency", "currency"],
            "subclass": ["crypto", "fiat"],
            "asset": ["ABC", "EUR"],
            "wallet": ["spot / main", "spot / main"],
            "amount": [-10.0, 20.0],
            "fee": [0.0, 0.2],
            "balance": [0.0, 19.8],
            "amountusd": [-21.0, 21.0],
        }
    )
    ledger = preprocess_kraken_ledgers(ledgers)
    assert ledger["type"].tolist() == ["trade", "trade"]
    assert _balances(ledger) == {"ABC": -10.0, "EUR": 19.8}


@pytest.mark.ut
def test_combine_export_files_drops_rows_of_overlapping_exports():
    fill = {"Date(UTC)": "2020-01-01 10:00:00", "Pair": "ABCBTC", "Executed": "10ABC"}
    first = pd.DataFrame([fill, fill])  # two identical fills in one export
    second = pd.DataFrame([fill, {**fill, "Executed": "20ABC"}])
    combined = combine_export_files([first, second])
    assert combined["Executed"].tolist() == ["10ABC", "10ABC", "20ABC"]


@pytest.mark.ut
def test_preprocess_binance_trades_splits_buys_sells_and_fees():
    trades = pd.DataFrame(
        {
            "Date(UTC)": ["2020-01-01 10:00:00", "2020-01-02 10:00:00"],
            "Pair": ["ABCBNB", "BNBEUR"],
            "Side": ["BUY", "SELL"],
            "Price": [0.0001, 100.0],
            "Executed": ["10,000.0000000000ABC", "1.0000000000BNB"],
            "Amount": ["1.00000000BNB", "100.00000000EUR"],
            "Fee": ["10.0000000000ABC", "0.0010000000BNB"],
        }
    )
    ledger = preprocess_binance_trades(trades)
    assert list(ledger.columns) == LEDGER_COLUMNS
    assert _balances(ledger) == {"ABC": 9990.0, "BNB": -2.001, "EUR": 100.0}
    # one refid per fill, shared by its legs
    assert ledger.groupby("refid").size().tolist() == [3, 3]


@pytest.mark.ut
def test_preprocess_kucoin_converts_to_utc_and_charges_fee_on_received_coin():
    trades = pd.DataFrame(
        {
            "oid": ["a1"],
            "symbol": ["ABC-BTC"],
            "dealPrice": [0.001],
            "dealValue": [0.01],
            "amount": [10.0],
            "fee": [0.01],
            "direction": ["BUY"],
            "createdDate": ["2020-01-01 18:00:00.0"],
        }
    )
    ledger = preprocess_kucoin_trades(trades)
    assert _balances(ledger) == {"ABC": 9.99, "BTC": -0.01}
    assert (ledger["date"] == pd.Timestamp("2020-01-01 10:00:00")).all()

    deposits = pd.DataFrame(
        {"Time": ["2020-01-01 17:00:00"], "Coin": ["ABC"], "Amount": [5.0]}
    )
    deposit = preprocess_kucoin_deposits(deposits)
    assert deposit["date"].tolist() == [pd.Timestamp("2020-01-01 09:00:00")]
    assert deposit["type"].tolist() == ["deposit"]


@pytest.mark.ut
def test_preprocess_manual_transactions_groups_trade_legs():
    manual = pd.DataFrame(
        {
            "date": ["2020-01-01 10:00:00"] * 2 + ["2020-01-02 10:00:00"],
            "exchange": ["wallet"] * 3,
            "type": ["trade", "trade", "removal"],
            "coin": ["ABC", "EUR", "EUR"],
            "amount": [-10.0, 20.0, -20.0],
            "fee": [0.0, 0.0, None],
            "group": ["sale", "sale", None],
            "comment": ["", "", ""],
        },
        index=[3, 4, 5],
    )
    ledger = preprocess_manual_transactions(manual)
    assert ledger["refid"].tolist() == ["manual-sale", "manual-sale", "manual-5"]
    assert ledger["fee"].tolist() == [0.0, 0.0, 0.0]


@pytest.mark.ut
def test_apply_conversions_pairs_transfers_into_one_trade():
    ledger = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"]),
            "exchange": ["kraken"] * 3,
            "refid": ["R-OUT", "R-IN", "R-FREE"],
            "type": ["reward"] * 3,
            "coin": ["ABC", "BTC", "XYZ"],
            "amount": [-1.0, 0.001, 5.0],
            "fee": [0.0] * 3,
        }
    )
    conversions = pd.DataFrame(
        {
            "exchange": ["kraken", "kraken"],
            "refid": ["R-OUT", "R-IN"],
            "group": ["abc-delisting", "abc-delisting"],
        }
    )
    result = apply_conversions(ledger, conversions)
    assert result["type"].tolist() == ["trade", "trade", "reward"]
    assert result["refid"].tolist() == [
        "conversion-abc-delisting",
        "conversion-abc-delisting",
        "R-FREE",
    ]

    with pytest.raises(AssertionError, match="abc-delisting"):
        apply_conversions(ledger, conversions.assign(refid=["R-FREE", "R-IN"]))


@pytest.mark.ut
def test_extract_fiat_transfers_keeps_only_eur_deposits_and_withdrawals():
    ledger = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"]
            ),
            "exchange": ["kraken"] * 4,
            "refid": ["D1", "W1", "T1", "W2"],
            "type": ["deposit", "withdrawal", "trade", "withdrawal"],
            "coin": ["EUR", "EUR", "EUR", "ABC"],
            "amount": [100.0, -40.0, -50.0, -5.0],
            "fee": [0.0, 1.0, 0.5, 0.1],
        }
    )
    transfers = extract_fiat_transfers(ledger)
    assert list(transfers.columns) == ["date", "exchange", "amount"]
    # the withdrawal fee is charged on the exchange, not on the bank account
    assert transfers["amount"].tolist() == [100.0, -40.0]
