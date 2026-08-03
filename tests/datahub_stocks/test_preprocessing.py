import pandas as pd
import pytest

from src.datahub_stocks.transform.preprocessing import (
    apply_mergers,
    aggregate_monthly_shares,
    aggregate_monthly_investments,
    aggregate_monthly_rebalancing,
    preprocess_dividends,
    aggregate_monthly_dividends,
    preprocess_interest,
    aggregate_monthly_interest,
    preprocess_stock_trades,
    preprocess_rebalancing,
    combine_portfolio_values,
    apply_splits,
)


@pytest.fixture
def portfolio():
    return pd.DataFrame(
        {
            "Datum": pd.to_datetime(
                ["2024-01-15", "2024-02-10", "2024-03-20", "2024-01-05"]
            ),
            "isin": ["OLD1", "OLD1", "NEW1", "KEEP"],
            "shares": [10.0, 5.0, 20.0, 8.0],
        }
    )


@pytest.fixture
def mergers():
    return pd.DataFrame(
        {
            "isin_old": ["OLD1"],
            "isin_new": ["MERGED1"],
            "stocks_old": [2.0],
            "stocks_new": [1.0],
        }
    )


@pytest.mark.ut
def test_apply_mergers_replaces_isin(portfolio, mergers):
    result = apply_mergers(portfolio, mergers)
    assert "OLD1" not in result["isin"].values
    assert (result["isin"] == "MERGED1").sum() == 2


@pytest.mark.ut
def test_apply_mergers_adjusts_shares(portfolio, mergers):
    result = apply_mergers(portfolio, mergers)
    merged_rows = result[result["isin"] == "MERGED1"]
    assert merged_rows["shares"].tolist() == [5.0, 2.5]


@pytest.mark.ut
def test_apply_mergers_keeps_unaffected_rows(portfolio, mergers):
    result = apply_mergers(portfolio, mergers)
    keep_row = result[result["isin"] == "KEEP"]
    assert keep_row["shares"].iloc[0] == 8.0
    new_row = result[result["isin"] == "NEW1"]
    assert new_row["shares"].iloc[0] == 20.0


@pytest.mark.ut
def test_apply_mergers_no_matching_isin(portfolio):
    empty_mergers = pd.DataFrame(
        {
            "isin_old": ["NONEXISTENT"],
            "isin_new": ["WHATEVER"],
            "stocks_old": [1.0],
            "stocks_new": [1.0],
        }
    )
    result = apply_mergers(portfolio, empty_mergers)
    pd.testing.assert_frame_equal(result, portfolio)


@pytest.mark.ut
def test_aggregate_monthly_shares_cumulates():
    portfolio = pd.DataFrame(
        {
            "Datum": pd.to_datetime(["2024-01-15", "2024-01-20", "2024-03-10"]),
            "isin": ["A", "A", "A"],
            "shares": [10.0, 5.0, 3.0],
        }
    )
    result = aggregate_monthly_shares(portfolio)
    a_rows = result[result["isin"] == "A"].sort_values("date")
    cumulative = a_rows["cumulative_shares"].tolist()
    assert cumulative[0] == 15.0
    assert cumulative[-1] == 18.0


@pytest.mark.ut
def test_aggregate_monthly_shares_fills_gaps():
    portfolio = pd.DataFrame(
        {
            "Datum": pd.to_datetime(["2024-01-15", "2024-03-10"]),
            "isin": ["A", "A"],
            "shares": [10.0, 5.0],
        }
    )
    result = aggregate_monthly_shares(portfolio)
    a_rows = result[result["isin"] == "A"].sort_values("date")
    assert len(a_rows) == 3
    assert a_rows["cumulative_shares"].tolist() == [10.0, 10.0, 15.0]


@pytest.mark.ut
def test_aggregate_monthly_investments_sums_per_month():
    portfolio = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-15", "2024-01-20", "2024-03-10"]),
            "trade_type": ["buy", "buy", "buy"],
            "total_investment": [100.0, 50.0, 200.0],
            "cost": [1.0, 0.5, 2.0],
        }
    )
    result = aggregate_monthly_investments(portfolio)
    assert result.columns.tolist() == [
        "date",
        "expense_investment",
        "income_investment",
        "order_costs",
    ]
    jan = result[result["date"] == "2024-01-31"].iloc[0]
    assert jan["expense_investment"] == 150.0
    assert jan["income_investment"] == 0.0
    assert jan["order_costs"] == 1.5
    mar = result[result["date"] == "2024-03-31"].iloc[0]
    assert mar["expense_investment"] == 200.0
    assert mar["order_costs"] == 2.0


@pytest.mark.ut
def test_aggregate_monthly_investments_splits_buys_and_sells():
    portfolio = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-02-05", "2024-02-25"]),
            "trade_type": ["buy", "sell"],
            # buys are positive, sells negative in total_investment
            "total_investment": [300.0, -100.0],
            "cost": [1.0, 2.0],
        }
    )
    result = aggregate_monthly_investments(portfolio)
    feb = result[result["date"] == "2024-02-29"].iloc[0]
    assert feb["expense_investment"] == 300.0  # only the buy
    assert feb["income_investment"] == 100.0  # sell proceeds, positive
    assert feb["order_costs"] == 3.0  # order costs of both


@pytest.fixture
def dividends_raw():
    # mirrors the German-headed Dividends sheet layout
    return pd.DataFrame(
        {
            "Datum": ["15.1.2024", "20.1.2024", "10.3.2024"],
            "Betrag": [1.5, 2.5, 4.0],
            "Name": ["ETF A", "ETF B", "ETF A"],
            "ISIN": ["A", "B", "A"],
        }
    )


@pytest.mark.ut
def test_preprocess_dividends_renames_and_types(dividends_raw):
    result = preprocess_dividends(dividends_raw)
    assert result.columns.tolist() == ["date", "isin", "name", "dividend"]
    assert pd.api.types.is_datetime64_any_dtype(result["date"])
    assert result["dividend"].sum() == 8.0


@pytest.mark.ut
def test_aggregate_monthly_dividends_sums_per_month_and_isin(dividends_raw):
    result = aggregate_monthly_dividends(preprocess_dividends(dividends_raw))
    assert result.columns.tolist() == ["date", "isin", "name", "dividend"]
    jan_a = result[(result["date"] == "2024-01-31") & (result["isin"] == "A")].iloc[0]
    assert jan_a["dividend"] == 1.5
    jan_b = result[(result["date"] == "2024-01-31") & (result["isin"] == "B")].iloc[0]
    assert jan_b["dividend"] == 2.5
    mar_a = result[(result["date"] == "2024-03-31") & (result["isin"] == "A")].iloc[0]
    assert mar_a["dividend"] == 4.0
    # one row per (month, isin); no all-zero rows
    assert len(result) == 3


@pytest.fixture
def stock_dividends_raw():
    # individual stock dividends: same layout, but no ISIN (empty column)
    return pd.DataFrame(
        {
            "Datum": ["2.4.2021", "1.7.2021", "5.7.2021"],
            "Betrag": [1.51, 1.49, 0.03],
            "Name": ["Coca Cola", "Coca Cola", "Nvidia"],
            "ISIN": [float("nan"), float("nan"), float("nan")],
        }
    )


@pytest.mark.ut
def test_preprocess_dividends_handles_missing_isin(stock_dividends_raw):
    result = preprocess_dividends(stock_dividends_raw)
    # missing ISIN becomes an empty string, securities keep their name
    assert (result["isin"] == "").all()
    assert result["name"].tolist() == ["Coca Cola", "Coca Cola", "Nvidia"]


@pytest.mark.ut
def test_aggregate_monthly_dividends_groups_stocks_by_name(stock_dividends_raw):
    result = aggregate_monthly_dividends(preprocess_dividends(stock_dividends_raw))
    # July: Coca Cola and Nvidia stay separate rows despite the shared empty ISIN
    jul = result[result["date"] == "2021-07-31"].set_index("name")["dividend"]
    assert jul["Coca Cola"] == 1.49
    assert jul["Nvidia"] == 0.03


@pytest.mark.ut
def test_aggregate_monthly_dividends_combines_etf_and_stocks(
    dividends_raw, stock_dividends_raw
):
    combined = pd.concat(
        [
            preprocess_dividends(dividends_raw),
            preprocess_dividends(stock_dividends_raw),
        ],
        ignore_index=True,
    )
    result = aggregate_monthly_dividends(combined)
    # ETF rows (with ISIN) and stock rows (empty ISIN) coexist in one table
    assert (result["isin"] == "").any()
    assert (result["isin"] != "").any()
    assert round(result["dividend"].sum(), 2) == round(
        dividends_raw["Betrag"].sum() + stock_dividends_raw["Betrag"].sum(), 2
    )


@pytest.fixture
def interest_raw():
    # mirrors the German-headed Zinsen sheet layout; Tagesgeld accounts carry a
    # ``ZT`` ISIN prefix, Festgeld accounts a ``ZF`` prefix
    return pd.DataFrame(
        {
            "Datum": ["1.2.2023", "1.2.2023", "13.12.2023"],
            "Betrag": [3.19, 2.66, 80.40],
            "Anbieter": ["traderepublic", "weltsparen", "weltsparen"],
            "Kommentar": ["Zinsen", "Zinsen", "Festgeld"],
            "Name": ["Zinsen", "Zinsen WS Tagesgeld", "Zinsen WS Festgeld"],
            "ISIN": ["ZT0000", "ZT0001", "ZF0101"],
        }
    )


@pytest.mark.ut
def test_preprocess_interest_renames_and_categorizes(interest_raw):
    result = preprocess_interest(interest_raw)
    assert result.columns.tolist() == ["date", "category", "interest"]
    assert pd.api.types.is_datetime64_any_dtype(result["date"])
    # ISIN prefix maps to the deposit kind
    assert result["category"].tolist() == ["Tagesgeld", "Tagesgeld", "Festgeld"]
    assert result["interest"].sum() == pytest.approx(86.25)


@pytest.mark.ut
def test_preprocess_interest_rejects_unknown_isin_prefix(interest_raw):
    bad = interest_raw.copy()
    bad.loc[0, "ISIN"] = "XX9999"
    with pytest.raises(AssertionError, match="unknown account ISIN prefix"):
        preprocess_interest(bad)


@pytest.mark.ut
def test_aggregate_monthly_interest_sums_per_month_and_category(interest_raw):
    result = aggregate_monthly_interest(preprocess_interest(interest_raw))
    assert result.columns.tolist() == ["date", "category", "interest"]
    # Feb 2023: both Tagesgeld rows collapse into a single category total
    feb = result[result["date"] == "2023-02-28"].set_index("category")["interest"]
    assert feb["Tagesgeld"] == pytest.approx(3.19 + 2.66)
    dec = result[result["date"] == "2023-12-31"].set_index("category")["interest"]
    assert dec["Festgeld"] == pytest.approx(80.40)
    # one row per (month, category); no all-zero rows
    assert len(result) == 2


@pytest.fixture
def stock_trades_raw():
    # German-headed stock Buys/Sells layout: no ISIN, no explicit shares column
    return pd.DataFrame(
        {
            "Datum": ["7.1.2020", "6.6.2022"],
            "Art": ["Aktien", "Put Option"],
            "Kurs": [40.65, 3.25],
            "Betrag": [203.25, 227.50],
            "Kosten": [1.0, 6.4],
            "Name": ["Coca Cola", "S&P500 Put"],
            "ISIN": [float("nan"), float("nan")],
        }
    )


@pytest.mark.ut
def test_preprocess_stock_trades_buy_signs(stock_trades_raw):
    result = preprocess_stock_trades(stock_trades_raw, is_buy=True)
    # the security name is used as the ISIN identifier
    assert result["isin"].tolist() == ["Coca Cola", "S&P500 Put"]
    # buys keep the trade kind for later filtering
    assert result["type"].tolist() == ["Aktien", "Put Option"]
    coke = result[result["name"] == "Coca Cola"].iloc[0]
    assert coke["shares"] == pytest.approx(203.25 / 40.65)  # 5 shares
    assert coke["total_investment"] == 203.25  # positive money invested
    assert coke["cost"] == 1.0  # order fees stay positive
    assert coke["trade_type"] == "buy"


@pytest.mark.ut
def test_preprocess_stock_trades_sell_signs(stock_trades_raw):
    result = preprocess_stock_trades(stock_trades_raw, is_buy=False)
    coke = result[result["name"] == "Coca Cola"].iloc[0]
    # sells reduce holdings and the net invested amount
    assert coke["shares"] == pytest.approx(-203.25 / 40.65)
    assert coke["total_investment"] == -203.25
    assert coke["cost"] == 1.0
    assert coke["trade_type"] == "sell"


@pytest.mark.ut
def test_preprocess_stock_trades_feeds_shares_and_investments(stock_trades_raw):
    # the output plugs straight into the ISIN-keyed aggregations
    portfolio = preprocess_stock_trades(stock_trades_raw, is_buy=True)
    portfolio = portfolio[portfolio["type"] == "Aktien"]
    shares = aggregate_monthly_shares(portfolio)
    assert shares[shares["isin"] == "Coca Cola"]["cumulative_shares"].iloc[
        0
    ] == pytest.approx(5.0)

    put = preprocess_stock_trades(stock_trades_raw, is_buy=True)
    put = put[put["type"] == "Put Option"]
    investments = aggregate_monthly_investments(put)
    jun = investments[investments["expense_investment"] != 0].iloc[0]
    assert jun["expense_investment"] == 227.50
    assert jun["order_costs"] == 6.4


@pytest.mark.ut
def test_combine_portfolio_values_tags_security_type():
    etf = pd.DataFrame({"isin": ["IE1"], "name": ["ETF A"], "value": [100.0]})
    stock = pd.DataFrame(
        {"isin": ["Coca Cola"], "name": ["Coca Cola"], "value": [50.0]}
    )
    result = combine_portfolio_values(etf, stock)
    assert result["security_type"].tolist() == ["ETF", "Aktie"]
    assert len(result) == 2


@pytest.mark.ut
def test_preprocess_stock_trades_rounds_shares_to_integers():
    # 2.0129 shares (amount/price) must round to a whole share
    raw = pd.DataFrame(
        {
            "Datum": ["8.3.2021"],
            "Art": ["Aktien"],
            "Kurs": [77.50],
            "Betrag": [156.0],
            "Kosten": [1.0],
            "Name": ["BioNTech"],
            "ISIN": [float("nan")],
        }
    )
    result = preprocess_stock_trades(raw, is_buy=True)
    assert result["shares"].iloc[0] == 2.0


@pytest.fixture
def tesla_portfolio():
    # mirrors the real Tesla trades around the 1-to-3 split on 25.8.2022
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2022-06-06", "2022-12-06", "2022-12-12"]),
            "name": ["Tesla", "Tesla", "Tesla"],
            "isin": ["Tesla", "Tesla", "Tesla"],
            "shares": [1.0, -3.0, 3.0],  # buy pre-split, sell, buy (post-split)
        }
    )


@pytest.fixture
def tesla_split():
    return pd.DataFrame(
        {
            "name": ["Tesla"],
            "date": pd.to_datetime(["2022-08-25"]),
            "split_from": [1.0],
            "split_to": [3.0],
        }
    )


@pytest.mark.ut
def test_apply_splits_rebases_only_pre_split_shares(tesla_portfolio, tesla_split):
    result = apply_splits(tesla_portfolio, tesla_split)
    shares_by_date = result.set_index("date")["shares"]
    # pre-split buy is multiplied by 3, post-split trades stay unchanged
    assert shares_by_date[pd.Timestamp("2022-06-06")] == 3.0
    assert shares_by_date[pd.Timestamp("2022-12-06")] == -3.0
    assert shares_by_date[pd.Timestamp("2022-12-12")] == 3.0
    # net current holding after the split is 3 shares
    assert result["shares"].sum() == 3.0


@pytest.fixture
def rebalancing_raw():
    # mirrors the Rebalancing sheet layout (same as Sells): buys have a negative
    # amount and no explicit share count, sells a positive amount with shares.
    return pd.DataFrame(
        {
            "index": [float("nan")] * 3,
            "date": ["16.12.2022", "12.12.2022", "12.12.2022"],
            "type": ["ETF Sparplan"] * 3,
            "price": [40.0, 39.83, 37.305],
            # buy of 800 EUR, sell of 6810.93 (171 shares), sell of 3208.23 (86)
            "amount": [-800.0, 6810.93, 3208.23],
            "cost": [0.0, -1.0, -1.0],
            "depot": ["traderepublic"] * 3,
            "shares": [float("nan"), 171.0, 86.0],
            "name": ["ETF A", "ETF B", "ETF C"],
            "isin": ["A", "B", "C"],
            "_checkSharesEqualAmountDivPrice": [float("nan")] * 3,
        }
    )


@pytest.mark.ut
def test_preprocess_rebalancing_buy_and_sell_signs(rebalancing_raw):
    result = preprocess_rebalancing(rebalancing_raw)
    assert result.columns.tolist() == [
        "date",
        "isin",
        "name",
        "type",
        "total_investment",
        "cost",
        "shares",
        "trade_type",
    ]
    buy = result[result["isin"] == "A"].iloc[0]
    assert buy["trade_type"] == "buy"
    # buy: derived positive shares (800 / 40) and positive invested amount
    assert buy["shares"] == pytest.approx(20.0)
    assert buy["total_investment"] == pytest.approx(800.0)
    assert buy["cost"] == 0.0

    sell = result[result["isin"] == "B"].iloc[0]
    assert sell["trade_type"] == "sell"
    # sell: shares and invested amount negated so a cumulative sum reduces both
    assert sell["shares"] == pytest.approx(-171.0)
    assert sell["total_investment"] == pytest.approx(-6810.93)
    # order fee flipped to positive, matching the Buys/Sells convention
    assert sell["cost"] == pytest.approx(1.0)


@pytest.mark.ut
def test_preprocess_rebalancing_feeds_shares(rebalancing_raw):
    # buys add and sells remove real shares from the holdings
    portfolio = preprocess_rebalancing(rebalancing_raw)
    shares = aggregate_monthly_shares(portfolio)
    a = shares[shares["isin"] == "A"]["cumulative_shares"]
    assert a.iloc[0] == pytest.approx(20.0)
    # B was only sold (goes negative), so it drops out of the positive holdings
    assert (shares["isin"] == "B").sum() == 0


@pytest.mark.ut
def test_aggregate_monthly_rebalancing_nets_to_expense(rebalancing_raw):
    result = aggregate_monthly_rebalancing(preprocess_rebalancing(rebalancing_raw))
    assert result.columns.tolist() == [
        "date",
        "expense_rebalancing",
        "income_rebalancing",
    ]
    dec = result[result["date"] == "2022-12-31"].iloc[0]
    # net = sell proceeds (6810.93 + 3208.23) - buy spend (800) - fees (2.0)
    #     = 9217.16 -> a net income
    assert dec["income_rebalancing"] == pytest.approx(9217.16)
    assert dec["expense_rebalancing"] == 0.0


@pytest.mark.ut
def test_aggregate_monthly_rebalancing_expense_when_buys_dominate():
    portfolio = preprocess_rebalancing(
        pd.DataFrame(
            {
                "date": ["16.12.2022", "12.12.2022"],
                "type": ["ETF Sparplan"] * 2,
                "price": [40.0, 39.83],
                "amount": [-5000.0, 1000.0],  # big buy, small sell
                "cost": [0.0, -1.0],
                "shares": [float("nan"), 25.11],
                "name": ["ETF A", "ETF B"],
                "isin": ["A", "B"],
            }
        )
    )
    result = aggregate_monthly_rebalancing(portfolio)
    dec = result[result["date"] == "2022-12-31"].iloc[0]
    # net = 1000 - 5000 - 1 = -4001 -> a net expense, reported as +4001
    assert dec["expense_rebalancing"] == pytest.approx(4001.0)
    assert dec["income_rebalancing"] == 0.0


@pytest.mark.ut
def test_apply_splits_ignores_other_securities(tesla_split):
    portfolio = pd.DataFrame(
        {
            "date": pd.to_datetime(["2022-06-06"]),
            "name": ["Coca Cola"],
            "isin": ["Coca Cola"],
            "shares": [5.0],
        }
    )
    result = apply_splits(portfolio, tesla_split)
    assert result["shares"].iloc[0] == 5.0
