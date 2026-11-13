"""Cash balance sheet: where cash lies, at which interest rate, and how long
fixed-term deposits keep it bound.

The manually maintained source workbook has three sheets with German headers:

- ``Konten``: one row per account (Giro, Tagesgeld, Verrechnung, Sparbuch) with
  its *current* interest rate (no history; received interest is tracked in the
  `Zinsen` sheet of the stocks trades file, not here).
- ``Kontostaende``: account balances at arbitrary dates (usually month end).
- ``Festgeld``: one row per fixed-term deposit contract.

Nothing here calculates interest amounts; rates are passed through for display.
"""

import logging

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)

ACCOUNT_TYPES = {"Giro", "Tagesgeld", "Verrechnung", "Sparbuch"}

# A balance older than this (relative to the month end, or today for the
# current month) is flagged as stale
STALE_AFTER_DAYS = 35

STATUS_RUNNING = "läuft"
STATUS_MATURED = "abgelaufen"
STATUS_UPCOMING = "zukünftig"

BUCKET_AVAILABLE = "verfügbar"
BUCKET_BOUND = "gebunden"

ACCOUNT_COLUMN_MAP = {
    "Konto-ID": "account_id",
    "Bank": "bank",
    "Plattform": "platform",
    "Name": "name",
    "Typ": "type",
    "Zinssatz": "rate_pa",
    "Zinssatz gültig bis": "rate_valid_until",
    "Eröffnet": "opened",
    "Geschlossen": "closed",
    "Kommentar": "comment",
}
ACCOUNT_REQUIRED_COLUMNS = {"Konto-ID", "Bank", "Name", "Typ"}

BALANCE_COLUMN_MAP = {
    "Datum": "date",
    "Konto-ID": "account_id",
    "Saldo": "balance",
    "Kommentar": "comment",
}
BALANCE_REQUIRED_COLUMNS = {"Datum", "Konto-ID", "Saldo"}

DEPOSIT_COLUMN_MAP = {
    "Vertrags-ID": "deposit_id",
    "Bank": "bank",
    "Plattform": "platform",
    "Name": "name",
    "Betrag": "principal",
    "Zinssatz": "rate_pa",
    "Beginn": "start_date",
    "Ende": "end_date",
    "Auszahlungskonto": "payout_account_id",
    "Kommentar": "comment",
}
DEPOSIT_REQUIRED_COLUMNS = {
    "Vertrags-ID",
    "Bank",
    "Betrag",
    "Zinssatz",
    "Beginn",
    "Ende",
}

MONTHLY_BALANCE_COLUMNS = [
    "date",
    "account_id",
    "bank",
    "platform",
    "name",
    "type",
    "rate_pa",
    "rate_valid_until",
    "balance",
    "balance_date",
    "is_stale",
]
TERM_DEPOSIT_COLUMNS = [
    "deposit_id",
    "bank",
    "platform",
    "name",
    "principal",
    "rate_pa",
    "start_date",
    "end_date",
    "payout_account_id",
    "status",
    "term_days",
    "days_remaining",
    "progress_pct",
    "comment",
]
LIQUIDITY_COLUMNS = ["date", "bucket", "value", "is_projection"]


# --- Preprocessing -------------------------------------------------------------


def _normalise_sheet(
    data: pd.DataFrame, column_map: dict, required: set, sheet: str
) -> pd.DataFrame:
    """Checks the required German headers, drops empty spreadsheet rows, renames
    the columns to English and adds the optional columns that are missing."""
    missing = required - set(data.columns)
    assert not missing, f"Columns missing in sheet `{sheet}`: {sorted(missing)}"
    data = data.dropna(how="all")
    data = data.rename(columns=column_map)
    for column in column_map.values():
        if column not in data.columns:
            data[column] = np.nan
    data = data[list(column_map.values())].copy()
    for column in data.columns:
        if data[column].dtype == object:
            data[column] = data[column].map(
                lambda v: (v.strip() or np.nan) if isinstance(v, str) else v
            )
    return data.reset_index(drop=True)


def _to_date(values: pd.Series) -> pd.Series:
    """Spreadsheet date cells arrive as timestamps, dates typed as text as
    ``dd.mm.yyyy``; empty cells become NaT."""
    if pd.api.types.is_datetime64_any_dtype(values):
        return values.dt.normalize()
    return pd.to_datetime(values, format="%d.%m.%Y").dt.normalize()


def _to_rate(values: pd.Series, sheet: str) -> pd.Series:
    """Rates come from percentage cells (4 % -> 0.04). A value above 1 means the
    cell was not formatted as percentage, which would read as e.g. 400 %."""
    rates = pd.to_numeric(values, errors="raise").astype(float)
    too_high = rates[rates > 1]
    assert too_high.empty, (
        f"Interest rates above 100 % in sheet `{sheet}` ({too_high.tolist()}); "
        "format the `Zinssatz` cells as percentage"
    )
    return rates


def _assert_unique(data: pd.DataFrame, columns: list, sheet: str) -> None:
    duplicated = data[data.duplicated(columns, keep=False)]
    assert duplicated.empty, (
        f"Duplicated {columns} in sheet `{sheet}`: "
        f"{duplicated[columns].drop_duplicates().values.tolist()}"
    )


def preprocess_accounts(data: pd.DataFrame) -> pd.DataFrame:
    """Normalises the ``Konten`` sheet."""
    accounts = _normalise_sheet(
        data, ACCOUNT_COLUMN_MAP, ACCOUNT_REQUIRED_COLUMNS, "Konten"
    )
    assert accounts["account_id"].notna().all(), "Account without `Konto-ID`"
    _assert_unique(accounts, ["account_id"], "Konten")
    unknown_types = sorted(set(accounts["type"].dropna()) - ACCOUNT_TYPES)
    assert not unknown_types and accounts["type"].notna().all(), (
        f"Unknown account `Typ` {unknown_types}, expected one of "
        f"{sorted(ACCOUNT_TYPES)}"
    )
    accounts["rate_pa"] = _to_rate(accounts["rate_pa"], "Konten")
    for column in ["rate_valid_until", "opened", "closed"]:
        accounts[column] = _to_date(accounts[column])
    return accounts


def preprocess_balances(data: pd.DataFrame) -> pd.DataFrame:
    """Normalises the ``Kontostaende`` sheet."""
    balances = _normalise_sheet(
        data, BALANCE_COLUMN_MAP, BALANCE_REQUIRED_COLUMNS, "Kontostaende"
    )
    balances["date"] = _to_date(balances["date"])
    balances["balance"] = pd.to_numeric(balances["balance"], errors="raise").astype(
        float
    )
    incomplete = balances[
        balances[["date", "account_id", "balance"]].isna().any(axis=1)
    ]
    assert (
        incomplete.empty
    ), f"Balances without date, account or amount: {incomplete.index.tolist()}"
    _assert_unique(balances, ["date", "account_id"], "Kontostaende")
    return balances.sort_values(["date", "account_id"]).reset_index(drop=True)


def preprocess_term_deposits(data: pd.DataFrame) -> pd.DataFrame:
    """Normalises the ``Festgeld`` sheet."""
    deposits = _normalise_sheet(
        data, DEPOSIT_COLUMN_MAP, DEPOSIT_REQUIRED_COLUMNS, "Festgeld"
    )
    assert deposits["deposit_id"].notna().all(), "Deposit without `Vertrags-ID`"
    _assert_unique(deposits, ["deposit_id"], "Festgeld")
    deposits["principal"] = pd.to_numeric(deposits["principal"], errors="raise").astype(
        float
    )
    deposits["rate_pa"] = _to_rate(deposits["rate_pa"], "Festgeld")
    deposits["start_date"] = _to_date(deposits["start_date"])
    deposits["end_date"] = _to_date(deposits["end_date"])
    invalid = deposits[~(deposits["end_date"] > deposits["start_date"])]
    assert (
        invalid.empty
    ), f"Deposits whose `Ende` is not after `Beginn`: {invalid['deposit_id'].tolist()}"
    return deposits.sort_values("end_date").reset_index(drop=True)


def validate_references(
    accounts: pd.DataFrame, balances: pd.DataFrame, deposits: pd.DataFrame
) -> None:
    """Every balance and every payout account must refer to a known account."""
    known = set(accounts["account_id"])
    unknown_balances = sorted(set(balances["account_id"]) - known)
    assert (
        not unknown_balances
    ), f"`Kontostaende` refer to unknown `Konto-ID`s: {unknown_balances}"
    unknown_payout = sorted(set(deposits["payout_account_id"].dropna()) - known)
    assert (
        not unknown_payout
    ), f"`Festgeld` refers to unknown `Auszahlungskonto`: {unknown_payout}"
    without_balance = sorted(known - set(balances["account_id"]))
    if without_balance:
        logger.warning(f"Accounts without any balance: {without_balance}")


# --- Calculation ---------------------------------------------------------------


def _month_ends(first: pd.Timestamp, last: pd.Timestamp) -> pd.DatetimeIndex:
    return pd.date_range(
        first + pd.offsets.MonthEnd(0), last + pd.offsets.MonthEnd(0), freq="ME"
    )


def calculate_monthly_balances(
    accounts: pd.DataFrame, balances: pd.DataFrame, today: pd.Timestamp
) -> pd.DataFrame:
    """Balance of every account per month end, from the account's first balance
    up to the current month.

    A month without its own balance carries the latest earlier one forward;
    `balance_date` tells which balance is used and `is_stale` flags it when it
    is older than ``STALE_AFTER_DAYS`` at the month end (or at `today` for the
    current month). Months after the month an account was closed are dropped.
    The interest rate is the account's *current* rate on every row.
    """
    if balances.empty:
        return pd.DataFrame(columns=MONTHLY_BALANCE_COLUMNS)
    today = pd.Timestamp(today).normalize()
    month_ends = _month_ends(balances["date"].min(), today)

    rows = []
    for account_id, account_balances in balances.groupby("account_id"):
        account_balances = account_balances.sort_values("date")
        latest = pd.merge_asof(
            pd.DataFrame({"date": month_ends}),
            account_balances[["date", "balance"]].assign(
                balance_date=account_balances["date"]
            ),
            on="date",
        ).dropna(subset=["balance_date"])
        latest["account_id"] = account_id
        rows.append(latest)
    monthly = pd.concat(rows, ignore_index=True)

    monthly = monthly.merge(
        accounts.drop(columns=["comment", "opened"]), on="account_id", how="left"
    )
    closed_month = monthly["closed"] + pd.offsets.MonthEnd(0)
    monthly = monthly[monthly["closed"].isna() | (monthly["date"] <= closed_month)]

    reference = monthly["date"].clip(upper=today)
    monthly["is_stale"] = (
        reference - monthly["balance_date"]
    ).dt.days > STALE_AFTER_DAYS
    return (
        monthly[MONTHLY_BALANCE_COLUMNS]
        .sort_values(["date", "account_id"])
        .reset_index(drop=True)
    )


def calculate_term_deposit_overview(
    deposits: pd.DataFrame, today: pd.Timestamp
) -> pd.DataFrame:
    """Status, term and remaining days of every fixed-term deposit as of
    `today`. A deposit is `läuft` from its start date until the day before its
    end date; on the end date the money is paid out (`abgelaufen`)."""
    today = pd.Timestamp(today).normalize()
    overview = deposits.copy()
    overview["status"] = STATUS_RUNNING
    overview.loc[overview["end_date"] <= today, "status"] = STATUS_MATURED
    overview.loc[overview["start_date"] > today, "status"] = STATUS_UPCOMING

    overview["term_days"] = (overview["end_date"] - overview["start_date"]).dt.days
    elapsed = (today - overview["start_date"]).dt.days.clip(
        lower=0, upper=overview["term_days"]
    )
    overview["days_remaining"] = overview["term_days"] - elapsed
    overview["progress_pct"] = (100 * elapsed / overview["term_days"]).round(1)
    return overview[TERM_DEPOSIT_COLUMNS].reset_index(drop=True)


def calculate_monthly_liquidity(
    monthly_balances: pd.DataFrame, deposits: pd.DataFrame, today: pd.Timestamp
) -> pd.DataFrame:
    """Cash per month end split into `verfügbar` (account balances) and
    `gebunden` (fixed-term deposits running at the month end).

    Past months and the current month use the recorded balances. The months
    after the current one, up to the last maturity, are a projection
    (`is_projection`): the current balances stay as they are and every deposit
    maturing after today moves its principal from `gebunden` to `verfügbar`.
    Months before the first recorded balance only have a `gebunden` value.
    """
    today = pd.Timestamp(today).normalize()
    current_month = today + pd.offsets.MonthEnd(0)

    starts = [current_month]
    ends = [current_month]
    if not monthly_balances.empty:
        starts.append(monthly_balances["date"].min())
    if not deposits.empty:
        starts.append(deposits["start_date"].min())
        ends.append(deposits["end_date"].max())
    month_ends = _month_ends(min(starts), max(ends))

    available = monthly_balances.groupby("date")["balance"].sum()
    current_available = available.get(current_month)

    rows = []
    for month_end in month_ends:
        running = (deposits["start_date"] <= month_end) & (
            deposits["end_date"] > month_end
        )
        bound = deposits.loc[running, "principal"].sum()
        is_projection = month_end > current_month
        if is_projection:
            paid_back = (deposits["end_date"] > today) & (
                deposits["end_date"] <= month_end
            )
            value = (
                None
                if current_available is None
                else current_available + deposits.loc[paid_back, "principal"].sum()
            )
        else:
            value = available.get(month_end)
        if value is not None:
            rows.append((month_end, BUCKET_AVAILABLE, float(value), is_projection))
        rows.append((month_end, BUCKET_BOUND, float(bound), is_projection))
    return pd.DataFrame(rows, columns=LIQUIDITY_COLUMNS)
