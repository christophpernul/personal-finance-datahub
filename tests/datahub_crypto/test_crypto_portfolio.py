import pandas as pd
import pytest

from src.datahub_crypto.transform.portfolio import (
    COIN_COLUMNS,
    PORTFOLIO_COLUMNS,
    calculate_cost_basis,
    calculate_current_holdings,
    aggregate_monthly_holdings,
    calculate_coin_overview,
    calculate_crypto_portfolio_value,
)


def _row(date, exchange, refid, row_type, coin, amount, fee=0.0):
    return {
        "date": pd.Timestamp(date),
        "exchange": exchange,
        "refid": refid,
        "type": row_type,
        "coin": coin,
        "amount": amount,
        "fee": fee,
    }


@pytest.fixture
def ledger():
    return pd.DataFrame(
        [
            # 1000 EUR deposited, 2 BTC bought for 1000 EUR (fee 10 EUR)
            _row("2020-01-05", "kraken", "D1", "deposit", "EUR", 1010.0),
            _row("2020-01-06", "kraken", "T1", "trade", "EUR", -1000.0, 10.0),
            _row("2020-01-06", "kraken", "T1", "trade", "BTC", 2.0),
            # 1 BTC moved to binance (fee 0.01 BTC)
            _row("2020-02-01", "kraken", "W1", "withdrawal", "BTC", -1.0, 0.01),
            _row("2020-02-01", "binance", "D2", "deposit", "BTC", 1.0),
            # 0.5 BTC swapped into 100 ADA, fee 1 ADA
            _row("2020-03-01", "binance", "T2", "trade", "BTC", -0.5),
            _row("2020-03-01", "binance", "T2", "trade", "ADA", 100.0, 1.0),
            # 0.99 BTC sold for 990 EUR on kraken, fee 1 EUR
            _row("2020-04-01", "kraken", "T3", "trade", "BTC", -0.99),
            _row("2020-04-01", "kraken", "T3", "trade", "EUR", 990.0, 1.0),
            # 989 EUR withdrawn to the bank, airdrop of 50 FLR
            _row("2020-05-01", "kraken", "W2", "withdrawal", "EUR", -989.0),
            _row("2020-05-02", "kraken", "R1", "reward", "FLR", 50.0),
        ]
    )


@pytest.mark.ut
def test_calculate_cost_basis_follows_average_cost(ledger):
    trades, coins = calculate_cost_basis(ledger)
    coins = coins.set_index("coin")

    # 1000 EUR for 2 BTC, the withdrawal fee consumes 0.01 BTC = 5 EUR cost
    assert coins.loc["BTC", "amount"] == pytest.approx(0.5)
    assert coins.loc["BTC", "total_cost"] == pytest.approx(10.0 + 5.0 + 1.0)
    # the swap carries 0.5 BTC * 500 EUR over to ADA; the ADA fee eats 1%
    assert coins.loc["ADA", "amount"] == pytest.approx(99.0)
    assert coins.loc["ADA", "total_investment"] == pytest.approx(247.5)
    assert coins.loc["ADA", "total_cost"] == pytest.approx(2.5)
    # selling 0.99 BTC at 500 EUR average cost for 990 EUR realizes 495 EUR
    assert coins.loc["BTC", "realized_gain"] == pytest.approx(495.0)
    # the remaining 0.5 BTC keep their 500 EUR average cost
    assert coins.loc["BTC", "total_investment"] == pytest.approx(250.0)
    # airdrops come in at zero cost
    assert coins.loc["FLR", "total_investment"] == 0.0
    assert coins.loc["EUR", "amount"] == pytest.approx(0.0, abs=1e-9)

    assert trades["refid"].tolist() == ["T1", "T2", "T3"]
    assert trades["cost_basis"].tolist() == pytest.approx([1000.0, 250.0, 495.0])


@pytest.mark.ut
def test_calculate_cost_basis_conserves_the_money_paid_in(ledger):
    _, coins = calculate_cost_basis(ledger)
    net_deposits = 1010.0 - 989.0
    assert (
        coins["total_investment"].sum()
        + coins["total_cost"].sum()
        - coins["realized_gain"].sum()
    ) == pytest.approx(net_deposits)


@pytest.mark.ut
def test_calculate_cost_basis_rejects_trades_without_two_coins(ledger):
    broken = ledger[~((ledger["refid"] == "T1") & (ledger["coin"] == "BTC"))]
    with pytest.raises(ValueError, match="T1"):
        calculate_cost_basis(broken.reset_index(drop=True))


@pytest.mark.ut
def test_holdings_are_split_by_exchange(ledger):
    holdings = calculate_current_holdings(ledger)
    by_position = holdings.set_index(["exchange", "coin"])["amount"]
    assert by_position[("binance", "BTC")] == pytest.approx(0.5)
    assert by_position[("binance", "ADA")] == pytest.approx(99.0)
    assert by_position[("kraken", "FLR")] == pytest.approx(50.0)
    # emptied positions (kraken BTC/EUR) are dropped
    assert ("kraken", "BTC") not in by_position.index


@pytest.mark.ut
def test_aggregate_monthly_holdings_runs_until_the_given_month(ledger):
    monthly = aggregate_monthly_holdings(ledger, until=pd.Timestamp("2020-07-15"))
    btc_kraken = monthly[(monthly["exchange"] == "kraken") & (monthly["coin"] == "BTC")]
    assert btc_kraken["date"].dt.strftime("%Y-%m").tolist() == [
        "2020-01",
        "2020-02",
        "2020-03",
    ]
    assert btc_kraken["amount"].tolist() == pytest.approx([2.0, 0.99, 0.99])
    assert monthly["date"].max() == pd.Timestamp("2020-07-31")


@pytest.mark.ut
def test_portfolio_value_splits_coin_cost_onto_exchanges():
    coins = pd.DataFrame(
        {
            "coin": ["BTC", "FLR"],
            "amount": [2.0, 50.0],
            "total_investment": [1000.0, 0.0],
            "total_cost": [10.0, 0.0],
            "realized_gain": [0.0, 0.0],
            "removed_cost": [0.0, 0.0],
        }
    )
    holdings = pd.DataFrame(
        {
            "exchange": ["kraken", "binance", "kraken"],
            "coin": ["BTC", "BTC", "FLR"],
            "amount": [1.5, 0.5, 50.0],
        }
    )
    prices = pd.DataFrame(
        {
            "coin": ["BTC", "FLR"],
            "date": [pd.Timestamp("2020-12-31")] * 2,
            "price": [1000.0, 0.01],
        }
    )
    master_data = pd.DataFrame(
        {
            "coin": ["BTC", "FLR"],
            "name": ["Bitcoin", "Flare"],
            "symbol": ["BTC-EUR", "FLR-USD"],
            "currency": ["EUR", "USD"],
        }
    )
    overview = calculate_coin_overview(coins, holdings, prices, master_data)
    assert list(overview.columns) == COIN_COLUMNS
    btc = overview.set_index("coin").loc["BTC"]
    assert btc["value"] == pytest.approx(2000.0)
    assert btc["value_gained"] == pytest.approx(990.0)
    assert btc["value_gained_pct"] == pytest.approx(99.0)
    # a free coin has no relative gain
    assert pd.isna(overview.set_index("coin").loc["FLR", "value_gained_pct"])

    portfolio = calculate_crypto_portfolio_value(overview, holdings)
    assert list(portfolio.columns) == PORTFOLIO_COLUMNS
    by_exchange = portfolio[portfolio["coin"] == "BTC"].set_index("exchange")
    assert by_exchange.loc["kraken", "total_investment"] == pytest.approx(750.0)
    assert by_exchange.loc["binance", "total_cost"] == pytest.approx(2.5)
    assert by_exchange["value"].sum() == pytest.approx(btc["value"])


@pytest.mark.ut
def test_calculate_cost_basis_removes_coins_without_booking_proceeds(ledger):
    removals = pd.DataFrame(
        [
            _row("2020-06-01", "binance", "X1", "removal", "ADA", -99.0),
            _row("2020-06-01", "kraken", "X2", "removal", "FLR", -50.0),
        ]
    )
    _, coins = calculate_cost_basis(pd.concat([ledger, removals], ignore_index=True))
    ada = coins.set_index("coin").loc["ADA"]
    assert ada["amount"] == pytest.approx(0.0, abs=1e-9)
    # the cost basis leaves with the coins, but no gain or loss is invented
    assert ada["total_investment"] == pytest.approx(0.0, abs=1e-9)
    assert ada["removed_cost"] == pytest.approx(247.5)
    assert ada["realized_gain"] == 0.0
    assert (
        coins["total_investment"].sum()
        + coins["total_cost"].sum()
        + coins["removed_cost"].sum()
        - coins["realized_gain"].sum()
    ) == pytest.approx(1010.0 - 989.0)
