import math

import pandas as pd
import pytest

from src.datahub_target.transform.calculate_sold_positions import (
    FLOW_COLUMNS,
    SOLD_POSITION_COLUMNS,
    allocate_ongoing_costs,
    assign_positions,
    build_sold_positions,
    xirr,
)


def _trades(rows):
    """rows: (date, isin, trade_type, shares, total_investment, cost)"""
    df = pd.DataFrame(
        rows,
        columns=["date", "isin", "trade_type", "shares", "total_investment", "cost"],
    )
    df["date"] = pd.to_datetime(df["date"])
    df["name"] = df["isin"] + " name"
    df["depot"] = "traderepublic"
    df["security_type"] = "ETF"
    return df


@pytest.fixture
def trades():
    return _trades(
        [
            # A: bought twice, sold in two orders (the rest a day later),
            # bought again (a new, still open position)
            ("2020-01-10", "A", "buy", 10.0, 100.0, 1.0),
            ("2020-06-10", "A", "buy", 10.0, 120.0, 1.0),
            ("2021-01-10", "A", "sell", -19.9, -199.0, 1.0),
            ("2021-01-11", "A", "sell", -0.1, -1.0, 0.0),
            ("2021-02-01", "A", "buy", 5.0, 50.0, 1.0),
            # B: still held after a partial sell
            ("2020-01-10", "B", "buy", 10.0, 100.0, 0.0),
            ("2020-03-10", "B", "sell", -4.0, -60.0, 0.0),
            # C: sold and bought back on the same day
            ("2020-01-10", "C", "buy", 2.0, 20.0, 0.0),
            ("2020-05-10", "C", "buy", 3.0, 30.0, 0.0),
            ("2020-05-10", "C", "sell", -2.0, -25.0, -1.0),
        ]
    )


@pytest.mark.ut
def test_assign_positions_numbers_positions_per_security(trades):
    result = assign_positions(trades).set_index(["isin", "date", "trade_type"])
    a = result.loc["A"]
    assert a["position_no"].tolist() == [1, 1, 1, 1, 2]
    # the rest of the sale a day later closes the position, not the first order
    assert a["closes_position"].tolist() == [False, False, False, True, False]
    assert (result.loc["B", "position_no"] == 1).all()
    assert not result.loc["B", "closes_position"].any()


@pytest.mark.ut
def test_assign_positions_takes_sells_before_buys_of_the_same_day(trades):
    c = assign_positions(trades).query("isin == 'C'")
    assert c.sort_values("date")["position_no"].tolist() == [1, 1, 2]
    closing = c[c["closes_position"]]
    assert closing["trade_type"].tolist() == ["sell"]


@pytest.mark.ut
def test_assign_positions_ids_combine_isin_and_number(trades):
    ids = set(assign_positions(trades)["position_id"])
    assert ids == {"A#1", "A#2", "B#1", "C#1", "C#2"}


@pytest.mark.ut
def test_allocate_ongoing_costs_splits_by_days_held_in_the_year():
    periods = pd.DataFrame(
        {
            "position_id": ["A#1", "A#2"],
            "isin": ["A", "A"],
            "open_date": pd.to_datetime(["2019-05-01", "2021-12-02"]),
            "end_date": pd.to_datetime(["2021-01-30", "2022-06-30"]),
        }
    )
    costs = pd.DataFrame(
        {
            "date": pd.to_datetime(["2021-12-31", "2020-12-31", "2021-12-31"]),
            "isin": ["A", "A", None],
            "cost": [6.0, 3.0, 100.0],
        }
    )
    result = allocate_ongoing_costs(costs, periods)
    by_position = result.groupby("position_id")["amount"].sum()
    # 2021: A#1 held 30 days, A#2 held 30 days -> 3.0 each; 2020 fully A#1
    assert by_position["A#1"] == pytest.approx(-6.0)
    assert by_position["A#2"] == pytest.approx(-3.0)
    # depot-wide costs without an ISIN belong to no position
    assert result["amount"].sum() == pytest.approx(-9.0)


@pytest.mark.ut
def test_allocate_ongoing_costs_of_a_year_not_held_go_to_the_last_position():
    periods = pd.DataFrame(
        {
            "position_id": ["A#1"],
            "isin": ["A"],
            "open_date": pd.to_datetime(["2019-05-01"]),
            "end_date": pd.to_datetime(["2020-03-01"]),
        }
    )
    costs = pd.DataFrame(
        {"date": pd.to_datetime(["2021-12-31"]), "isin": ["A"], "cost": [2.0]}
    )
    result = allocate_ongoing_costs(costs, periods)
    assert result["position_id"].tolist() == ["A#1"]
    assert result["amount"].tolist() == [-2.0]


@pytest.mark.ut
def test_xirr_of_a_doubling_after_one_year():
    dates = pd.to_datetime(["2020-01-01", "2020-12-31"])
    assert xirr(dates, [-100.0, 200.0]) == pytest.approx(100.0, abs=0.5)


@pytest.mark.ut
def test_xirr_needs_money_in_and_out():
    assert math.isnan(xirr(pd.to_datetime(["2020-01-01"]), [-100.0]))


@pytest.fixture
def sold(trades):
    dividends = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-07-01", "2021-01-20", "2020-02-01"]),
            "isin": ["A", "A", "B"],
            "name": ["A name", "A name", "B name"],
            "dividend": [5.0, 2.0, 1.0],
        }
    )
    costs = pd.DataFrame(
        {"date": pd.to_datetime(["2020-12-31"]), "isin": ["A"], "cost": [3.0]}
    )
    return build_sold_positions(
        trades=trades,
        dividends=dividends,
        dividend_keys=dividends["isin"],
        costs=costs,
        valuation_date=pd.Timestamp("2021-06-30"),
    )


@pytest.mark.ut
def test_build_sold_positions_lists_only_closed_positions(sold):
    positions, flows = sold
    assert list(positions.columns) == SOLD_POSITION_COLUMNS
    assert list(flows.columns) == FLOW_COLUMNS
    assert set(positions["position_id"]) == {"A#1", "C#1"}
    assert set(flows["position_id"]) == {"A#1", "C#1"}


@pytest.mark.ut
def test_build_sold_positions_values_every_flow(sold):
    a = sold[0].set_index("position_id").loc["A#1"]
    assert a["open_date"] == "2020-01-10"
    assert a["close_date"] == "2021-01-11"
    assert a["buy_amount"] == pytest.approx(220.0)
    assert a["sell_amount"] == pytest.approx(200.0)
    assert a["price_gain"] == pytest.approx(-20.0)
    # the dividend paid after the sale still belongs to the sold position
    assert a["dividends"] == pytest.approx(7.0)
    assert a["order_costs"] == pytest.approx(3.0)
    assert a["ongoing_costs"] == pytest.approx(3.0)
    assert a["total_costs"] == pytest.approx(6.0)
    assert a["net_profit"] == pytest.approx(200.0 + 7.0 - 220.0 - 6.0)
    assert a["roi_pct"] == pytest.approx(-19.0 / 220.0 * 100)
    assert a["annual_return_pct"] < 0
    assert a["rebought"]


@pytest.mark.ut
def test_build_sold_positions_counts_negative_order_costs_as_paid(sold):
    c = sold[0].set_index("position_id").loc["C#1"]
    assert c["order_costs"] == pytest.approx(1.0)
    assert c["net_profit"] == pytest.approx(25.0 - 20.0 - 1.0)


@pytest.mark.ut
def test_build_sold_positions_flows_sum_to_net_profit(sold):
    positions, flows = sold
    by_position = flows.groupby("position_id")["amount"].sum()
    for _, position in positions.iterrows():
        assert by_position[position["position_id"]] == pytest.approx(
            position["net_profit"]
        )
