import numpy as np
import pandas as pd
import pytest

from src.datahub_cash.transform.balance_sheet import (
    BUCKET_AVAILABLE,
    BUCKET_BOUND,
    BUCKET_LIABILITIES,
    LIQUIDITY_COLUMNS,
    MONTHLY_BALANCE_COLUMNS,
    STATUS_MATURED,
    STATUS_RUNNING,
    STATUS_UPCOMING,
    TERM_DEPOSIT_COLUMNS,
    calculate_monthly_balances,
    calculate_monthly_liquidity,
    calculate_term_deposit_overview,
    preprocess_accounts,
    preprocess_balances,
    preprocess_term_deposits,
    validate_references,
)


TODAY = pd.Timestamp("2026-11-13")


def _accounts_sheet(**overrides):
    """Raw `Konten` sheet as read from the spreadsheet (German headers)."""
    sheet = pd.DataFrame(
        {
            "Konto-ID": ["giro", "tagesgeld", "old"],
            "Bank": ["DKB", "DKB ", "Bank X"],
            "Plattform": [np.nan, np.nan, np.nan],
            "Name": ["Giro", "Tagesgeld", "Altes Konto"],
            "Typ": ["Giro", "Tagesgeld", "Giro"],
            "Zinssatz": [np.nan, 0.04, np.nan],
            "Zinssatz gültig bis": [pd.NaT, pd.Timestamp("2027-02-28"), pd.NaT],
            "Eröffnet": [np.nan, np.nan, np.nan],
            "Geschlossen": [pd.NaT, pd.NaT, pd.Timestamp("2026-09-15")],
            "Kommentar": [np.nan, np.nan, "geschlossen"],
        }
    )
    for column, values in overrides.items():
        sheet[column] = values
    return sheet


def _balances_sheet():
    return pd.DataFrame(
        {
            "Datum": pd.to_datetime(
                ["2026-08-31", "2026-08-31", "2026-08-31", "2026-11-13", "2026-11-13"]
            ),
            "Konto-ID": ["giro", "tagesgeld", "old", "giro", "tagesgeld"],
            "Saldo": [400, 7000, 50, 500, 7500],
            "Kommentar": [np.nan] * 5,
        }
    )


def _deposits_sheet():
    return pd.DataFrame(
        {
            "Vertrags-ID": ["fg_long", "fg_short", "fg_old"],
            "Bank": ["Creditplus", "HoistSpar", "Bank Y"],
            "Plattform": ["Raisin", "Raisin", np.nan],
            "Name": ["FG 3J", "FG 3J", "FG 1J"],
            "Betrag": [3000, 3232.33, 1000],
            "Zinssatz": [0.0265, 0.038, 0.03],
            "Beginn": pd.to_datetime(["2024-12-23", "2023-12-29", "2025-01-01"]),
            "Ende": pd.to_datetime(["2027-12-23", "2026-12-29", "2026-01-01"]),
            "Auszahlungskonto": ["giro", "giro", np.nan],
            "Kommentar": [np.nan] * 3,
        }
    )


@pytest.fixture
def accounts():
    return preprocess_accounts(_accounts_sheet())


@pytest.fixture
def balances():
    return preprocess_balances(_balances_sheet())


@pytest.fixture
def deposits():
    return preprocess_term_deposits(_deposits_sheet())


# --- Preprocessing -------------------------------------------------------------


@pytest.mark.ut
def test_preprocess_accounts_renames_and_cleans(accounts):
    assert list(accounts.columns) == [
        "account_id",
        "bank",
        "platform",
        "name",
        "type",
        "rate_pa",
        "rate_valid_until",
        "opened",
        "closed",
        "comment",
    ]
    assert accounts.loc[1, "bank"] == "DKB"
    assert accounts.loc[1, "rate_pa"] == pytest.approx(0.04)
    assert pd.api.types.is_datetime64_any_dtype(accounts["opened"])
    assert accounts["opened"].isna().all()


@pytest.mark.ut
def test_preprocess_accounts_drops_empty_rows_and_adds_optional_columns():
    sheet = _accounts_sheet()[["Konto-ID", "Bank", "Name", "Typ"]]
    sheet = pd.concat([sheet, pd.DataFrame([[np.nan] * 4], columns=sheet.columns)])

    accounts = preprocess_accounts(sheet)

    assert len(accounts) == 3
    assert accounts["rate_pa"].isna().all()
    assert accounts["closed"].isna().all()


@pytest.mark.ut
def test_preprocess_accounts_parses_text_dates():
    accounts = preprocess_accounts(
        _accounts_sheet(**{"Zinssatz gültig bis": [np.nan, "28.02.2027", np.nan]})
    )
    assert accounts.loc[1, "rate_valid_until"] == pd.Timestamp("2027-02-28")


@pytest.mark.ut
def test_preprocess_accounts_rejects_unknown_type():
    with pytest.raises(AssertionError, match="Unknown account `Typ`"):
        preprocess_accounts(_accounts_sheet(Typ=["Giro", "Depot", "Giro"]))


@pytest.mark.ut
def test_preprocess_accounts_rejects_rate_not_formatted_as_percentage():
    with pytest.raises(AssertionError, match="percentage"):
        preprocess_accounts(_accounts_sheet(Zinssatz=[np.nan, 4.0, np.nan]))


@pytest.mark.ut
def test_preprocess_accounts_rejects_missing_column():
    with pytest.raises(AssertionError, match="Konto-ID"):
        preprocess_accounts(_accounts_sheet().drop(columns="Konto-ID"))


@pytest.mark.ut
def test_preprocess_balances_rejects_duplicates():
    sheet = pd.concat([_balances_sheet(), _balances_sheet().iloc[[0]]])
    with pytest.raises(AssertionError, match="Duplicated"):
        preprocess_balances(sheet)


@pytest.mark.ut
def test_preprocess_term_deposits_rejects_end_before_start():
    sheet = _deposits_sheet()
    sheet.loc[0, "Ende"] = pd.Timestamp("2024-01-01")
    with pytest.raises(AssertionError, match="fg_long"):
        preprocess_term_deposits(sheet)


@pytest.mark.ut
def test_validate_references_rejects_unknown_accounts(accounts, balances, deposits):
    validate_references(accounts, balances, deposits)

    deposits.loc[0, "payout_account_id"] = "unknown"
    with pytest.raises(AssertionError, match="unknown"):
        validate_references(accounts, balances, deposits)


# --- Monthly balances ----------------------------------------------------------


@pytest.mark.ut
def test_monthly_balances_carry_forward_and_flag_stale(accounts, balances):
    monthly = calculate_monthly_balances(accounts, balances, TODAY)
    assert list(monthly.columns) == MONTHLY_BALANCE_COLUMNS
    giro = monthly[monthly["account_id"] == "giro"].set_index("date")

    assert list(giro.index) == list(
        pd.to_datetime(["2026-08-31", "2026-09-30", "2026-10-31", "2026-11-30"])
    )
    # September carries the August balance and is not yet outdated, October is
    assert giro.loc["2026-09-30", "balance"] == 400
    assert not giro.loc["2026-09-30", "is_stale"]
    assert giro.loc["2026-10-31", "is_stale"]
    # the current month uses the latest balance, judged against today
    assert giro.loc["2026-11-30", "balance"] == 500
    assert giro.loc["2026-11-30", "balance_date"] == pd.Timestamp("2026-11-13")
    assert not giro.loc["2026-11-30", "is_stale"]


@pytest.mark.ut
def test_monthly_balances_pass_current_rate_and_drop_closed_accounts(
    accounts, balances
):
    monthly = calculate_monthly_balances(accounts, balances, TODAY)

    old = monthly[monthly["account_id"] == "old"]
    assert old["date"].max() == pd.Timestamp("2026-09-30")
    tagesgeld = monthly[monthly["account_id"] == "tagesgeld"]
    assert (tagesgeld["rate_pa"] == 0.04).all()
    assert (tagesgeld["rate_valid_until"] == pd.Timestamp("2027-02-28")).all()


# --- Fixed-term deposits -------------------------------------------------------


@pytest.mark.ut
def test_term_deposit_overview(deposits):
    overview = calculate_term_deposit_overview(deposits, TODAY).set_index("deposit_id")
    assert list(overview.reset_index().columns) == TERM_DEPOSIT_COLUMNS

    assert overview.loc["fg_old", "status"] == STATUS_MATURED
    assert overview.loc["fg_old", "days_remaining"] == 0
    assert overview.loc["fg_old", "progress_pct"] == 100

    short = overview.loc["fg_short"]
    assert short["status"] == STATUS_RUNNING
    assert short["term_days"] == 1096
    assert short["days_remaining"] == 46
    assert short["progress_pct"] == pytest.approx(95.8)


@pytest.mark.ut
def test_term_deposit_overview_upcoming_and_on_maturity_day(deposits):
    overview = calculate_term_deposit_overview(
        deposits, pd.Timestamp("2023-06-01")
    ).set_index("deposit_id")
    assert overview.loc["fg_short", "status"] == STATUS_UPCOMING
    assert overview.loc["fg_short", "progress_pct"] == 0

    on_end = calculate_term_deposit_overview(
        deposits, pd.Timestamp("2026-12-29")
    ).set_index("deposit_id")
    assert on_end.loc["fg_short", "status"] == STATUS_MATURED


# --- Liquidity -----------------------------------------------------------------


@pytest.mark.ut
def test_monthly_liquidity(accounts, balances, deposits):
    monthly_balances = calculate_monthly_balances(accounts, balances, TODAY)
    liquidity = calculate_monthly_liquidity(monthly_balances, deposits, TODAY)
    assert list(liquidity.columns) == LIQUIDITY_COLUMNS
    values = liquidity.set_index(["date", "bucket"])["value"]

    def value(date, bucket):
        return values[(pd.Timestamp(date), bucket)]

    # before the first balance only the bound money is known
    assert (pd.Timestamp("2023-12-31"), BUCKET_AVAILABLE) not in values.index
    assert value("2023-12-31", BUCKET_BOUND) == pytest.approx(3232.33)
    # all three deposits run in mid 2025
    assert value("2025-06-30", BUCKET_BOUND) == pytest.approx(7232.33)
    # current month: recorded balances (closed account gone), two deposits bound
    assert value("2026-11-30", BUCKET_AVAILABLE) == pytest.approx(8000)
    assert value("2026-11-30", BUCKET_BOUND) == pytest.approx(6232.33)
    assert not liquidity.loc[liquidity["date"] == "2026-11-30", "is_projection"].any()
    # December: fg_short matures and becomes available (projection)
    assert liquidity.loc[liquidity["date"] == "2026-12-31", "is_projection"].all()
    assert value("2026-12-31", BUCKET_AVAILABLE) == pytest.approx(11232.33)
    assert value("2026-12-31", BUCKET_BOUND) == pytest.approx(3000)
    # the projection ends with the last maturity, when nothing is bound anymore
    assert liquidity["date"].max() == pd.Timestamp("2027-12-31")
    assert value("2027-12-31", BUCKET_AVAILABLE) == pytest.approx(14232.33)
    assert value("2027-12-31", BUCKET_BOUND) == 0
    # no liability account: nothing owed in every month with balances
    assert value("2026-11-30", BUCKET_LIABILITIES) == 0
    assert (pd.Timestamp("2023-12-31"), BUCKET_LIABILITIES) not in values.index


# --- Liabilities ---------------------------------------------------------------


def _with_liabilities(card_balance: float, loan_balance: float):
    accounts = _accounts_sheet()
    liabilities = pd.DataFrame(
        {
            "Konto-ID": ["card", "loan"],
            "Bank": ["DKB", "offen"],
            "Name": ["Kreditkarte", "Kredit"],
            "Typ": ["Kreditkarte", "Kredit"],
        }
    )
    balances = pd.concat(
        [
            _balances_sheet(),
            pd.DataFrame(
                {
                    "Datum": pd.to_datetime(["2026-11-13", "2026-11-13"]),
                    "Konto-ID": ["card", "loan"],
                    "Saldo": [card_balance, loan_balance],
                }
            ),
        ],
        ignore_index=True,
    )
    return (
        preprocess_accounts(pd.concat([accounts, liabilities], ignore_index=True)),
        preprocess_balances(balances),
    )


@pytest.mark.ut
def test_liabilities_are_kept_apart_from_available_cash(deposits):
    accounts, balances = _with_liabilities(-250.0, -1000.0)
    validate_references(accounts, balances, deposits)

    monthly = calculate_monthly_balances(accounts, balances, TODAY)
    flags = monthly.drop_duplicates("account_id").set_index("account_id")
    assert flags.loc["card", "is_liability"] and flags.loc["loan", "is_liability"]
    assert not flags.loc["giro", "is_liability"]

    liquidity = calculate_monthly_liquidity(monthly, deposits, TODAY)
    values = liquidity.set_index(["date", "bucket"])["value"]
    current = pd.Timestamp("2026-11-30")
    assert values[(current, BUCKET_AVAILABLE)] == pytest.approx(8000)
    assert values[(current, BUCKET_LIABILITIES)] == pytest.approx(-1250)
    # the projection keeps the current debt
    assert values[(pd.Timestamp("2027-06-30"), BUCKET_LIABILITIES)] == pytest.approx(
        -1250
    )


@pytest.mark.ut
def test_liabilities_reject_positive_balance(deposits):
    accounts, balances = _with_liabilities(250.0, 0.0)
    with pytest.raises(AssertionError, match="negative `Saldo`"):
        validate_references(accounts, balances, deposits)
