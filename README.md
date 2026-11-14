# personal-finance-datahub

Turns the finance data you collect (bank and app exports, spreadsheets you keep by hand) into
clean, dashboard-ready tables for the
[personal-finance-dashboard](https://github.com/christophpernul/personal-finance-dashboard).
The datahub does all the calculations; the dashboard only displays the result.

- [Quick start](#quick-start)
- [What you maintain by hand](#what-you-maintain-by-hand) — the routine checklist and every input file
- [How the pipeline works](#how-the-pipeline-works) — stages and output tables
- [Calculations](#calculations) — how cashflow, portfolio, returns, crypto, cash and Riester are computed
- [Manual transactions](#manual-transactions) and [special cases](#special-cases)

## Quick start

All data lives outside this repository in the **datahub folder**
`D:\SynologyDrive\Finance\data\datahub\` (set as `DATAHUB_ROOT_FILEPATH` in `src/constants.py`):

```
datahub/
├── source/      ← your inputs: exports and hand-maintained files (see below)
├── transform/   ← generated: cleaned and calculated tables
└── target/      ← generated: dashboard-ready cashflow and sold-position tables
```

Run the whole pipeline from the `src` folder (it needs the repository's virtual environment):

```bash
cd src
../.venv/Scripts/python.exe run.py
```

It runs the stages cashflow → stocks → crypto → cash → target, fetches current prices from Yahoo
Finance, and logs to the console and `src/datahub.log`. Warnings name data it had to leave out
(e.g. a price Yahoo no longer delivers); an error stops the run and says which input to fix.

## What you maintain by hand

Everything below is in the datahub folder. Never edit `transform/` or `target/`: they are
overwritten on every run.

### Routine checklist

**Every month** (then run the pipeline):

1. Export the month from **Toshl** into `source/cashflow/toshl/` (see [Toshl](#cashflow-toshl)).
2. Enter **new ETF trades, dividends and received interest** in
   `source/stocks/source_stocks_portfolio_trades.ods`, **stock trades** in
   `source/stocks/source_stocks_trades.ods`.
3. Enter the **account balances** at month end in the `Kontostaende` sheet of
   `source/cash/source_cash_balance_sheet.ods`.
4. Drop new **crypto exchange exports** into `source/crypto/` (only if you traded or moved coins).

**Now and then:**

| When | What to do |
| --- | --- |
| You see a new Riester value at Raisin | Add a row to `source/stocks/source_stocks_riester_value.ods` |
| Raisin charged new Riester fees | Replace `source/stocks/raisin_riester_gebuehren.csv` with a new export |
| A new comdirect cost report / Trade Republic fees | Add the rows to `source/stocks/comdirect_costs_combined.csv` |
| You buy a **new ETF** | Run `python -m datahub_stocks.init_master_data` from `src`, then complete the new row in `source/source_etf__master_data.csv` |
| You buy a **new stock** | Add it to `source/stocks/source_stocks__master_data.csv` |
| An ETF is merged / a stock is split | Add a row to `source_stock_mergers.ods` / `source_stock_splits.ods` |
| You open/close an account, a Festgeld starts, an interest rate changes | Update `source/cash/source_cash_balance_sheet.ods` |
| You hold a **new coin** | Add it to `source/crypto/source_crypto__master_data.csv` |
| You use a **new Toshl tag** | Add it to `toshl_tag_categorization.json` (in this repository) — otherwise the run stops |
| Data is missing in an export or booked twice | Add a correction to `source/source_manual__transactions.csv` |

### Input files

| Area | File (in `source/`) | Kind | Feeds |
| --- | --- | --- | --- |
| Cashflow | `cashflow/toshl/bilanz_*.csv` | Toshl export | incomes and expenses |
| Cashflow | `toshl_tag_categorization.json` (repository root) | by hand | categories of incomes and expenses |
| ETFs | `stocks/source_stocks_portfolio_trades.ods` | by hand | ETF portfolio, dividends, interest, Riester contributions |
| Stocks | `stocks/source_stocks_trades.ods` | by hand | stock portfolio and dividends |
| ETFs/stocks | `stocks/source_stock_mergers.ods`, `stocks/source_stock_splits.ods` | by hand | share counts after mergers/splits |
| ETFs/stocks | `stocks/comdirect_costs_combined.csv` | bank reports, combined by hand | `Investment Costs` expenses, ongoing costs of sold positions |
| ETFs | `source_etf__master_data.csv` | generated once, completed by hand | Yahoo symbol and currency for prices |
| Stocks | `stocks/source_stocks__master_data.csv` | by hand | Yahoo symbol and currency for prices |
| Riester | `stocks/raisin_riester_gebuehren.csv` | Raisin export | Riester costs |
| Riester | `stocks/source_stocks_riester_value.ods` | by hand | Riester value |
| Crypto | `crypto/kraken[-_]ledgers[-_]*.csv`, `crypto/binance_tradehistory_*.csv`, `crypto/binance_deposits_*.xlsx`, `crypto/kucoin_trades_*.csv`, `crypto/kucoin_deposits_*.csv` | exchange exports | crypto portfolio, `Crypto` cashflow |
| Crypto | `crypto/source_crypto__master_data.csv` | by hand | Yahoo symbol for prices |
| Cash | `cash/source_cash_balance_sheet.ods` | by hand | balances, fixed-term deposits, liabilities |
| All | `source_manual__transactions.csv` | by hand | corrections of the exports |

Written by the pipeline, don't edit: `source_etf__price_current.csv`,
`source_etf__price_historic.csv`, `source_etf__market_snapshot.csv`. Not read by the pipeline:
`consumption/`, `cashflow/userinput/`, `stocks/missing_prices.ods`, `stocks/Transaktionsexport.csv`.

Spreadsheet conventions: dates as `dd.mm.yyyy` (a date cell or text), interest rates as
percentage cells (4 % — a plain `4` would read as 400 % and stops the run), CSV files with `;` and
decimal comma unless noted otherwise.

#### Cashflow: Toshl

- **`cashflow/toshl/bilanz_YYYY_MM.csv`** — one Toshl export per month (every file matching
  `bilanz_*.csv` is read). Columns as exported: `Date` (M/D/YY), `Account`, `Category`, `Tags`,
  `Expense amount`, `Income amount`, `Currency`, `In main currency`, `Main currency`,
  `Description`. Use exactly one tag per entry; the EUR amount (`In main currency`) is used.
- **`toshl_tag_categorization.json`** (in this repository) — maps every Toshl tag to a dashboard
  category, separately for `expenses` and `income`:
  `{"expenses": {"Lebensmittel": ["Lebensmittel", "groceries"], ...}, "income": {...}}`.
  A tag that is in no list stops the run with the list of uncategorized tags.

#### ETFs: `stocks/source_stocks_portfolio_trades.ods`

| Sheet | Columns | Enter |
| --- | --- | --- |
| `Buys` | `index`, `date`, `type`, `price`, `amount`, `cost`, `depot`, `comment`, `name`, `isin`, `note` | one row per buy (also each savings-plan execution); `amount` and `cost` negative as on the bank statement; shares are `amount / price` |
| `Sells` | `index`, `date`, `type`, `price`, `amount`, `cost`, `depot`, `shares`, `name`, `isin`, `_checkSharesEqualAmountDivPrice` | one row per sell; `amount` = proceeds (positive), `shares` = shares sold |
| `Rebalancing` | same as `Sells` | buys (negative `amount`) and sells (positive `amount`, `shares`) done only to rebalance; only their monthly net counts as cashflow |
| `Dividends` | `Datum`, `Betrag`, `Name`, `ISIN` (+ more, ignored) | one row per distribution received |
| `Zinsen` | `Datum`, `Betrag`, `ISIN` (+ more, ignored) | one row per interest payment received; `ISIN` is your own account code: `ZT…` = Tagesgeld, `ZF…` = Festgeld (any other prefix stops the run) |
| `Riester Buy` | `Datum`, `Art`, `Betrag` | one row per Riester contribution (`Riester Monatsbeitrag` or `Riester Einmalzahlung`) |

`depot` is the broker id (`traderepublic`, `comdirect`, …).

#### Stocks: `stocks/source_stocks_trades.ods`

Sheets `Buys`, `Sells` and `Dividends` with German headers `Datum`, `Art`, `Kurs`, `Betrag`,
`Kosten`, `Anbieter`, `Kommentar`, `Name`, `ISIN`. Stocks are identified by `Name` (no ISIN),
`Betrag` and `Kosten` are positive, shares are `Betrag / Kurs` rounded to whole shares. Only rows
with `Art` = `Aktien` are holdings; anything else (e.g. an expired `Put Option`) is booked as a
pure investment expense.

#### ETFs/stocks: master data, mergers, splits, costs

- **`source_etf__master_data.csv`** — `isin;name;symbol;type;currency;exchange_name;ter;region;distribution;replication;etf_type;comment`.
  `python -m datahub_stocks.init_master_data` (from `src`) creates it from the ISINs in the trade
  sheets with what Yahoo knows; fill in `replication`, `etf_type` and `comment`, and check
  `symbol` (the Yahoo ticker used for prices) and `currency`. An ETF without symbol gets no price.
- **`stocks/source_stocks__master_data.csv`** — `name;symbol;currency;type;ter`, one row per
  stock, `name` spelled as in the trade sheets, `symbol` the Yahoo ticker (e.g. `AMZN`).
- **`stocks/source_stock_mergers.ods`** — `isin_old`, `isin_new`, `name_old`, `name_new`, `type`,
  `date`, `stocks_old`, `stocks_new`: an ETF replaced by another; trades, dividends and costs of
  the old ISIN move to the new one, shares × `stocks_new / stocks_old`.
- **`stocks/source_stock_splits.ods`** — `name`, `date`, `split_from`, `split_to`: shares bought
  before `date` × `split_to / split_from` (Tesla 1→3: `1`, `3`).
- **`stocks/comdirect_costs_combined.csv`** — comma-separated, dot decimals:
  `Datum,Kosten,Anbieter,Kommentar,Name,ISIN,Note`. Costs as positive amounts (comdirect yearly
  Wertpapierkosten/Depotentgelt per security, Trade Republic fees); a credit is negative. Rows with
  an ISIN are also the ongoing costs of that security.

#### Riester

- **`stocks/raisin_riester_gebuehren.csv`** — the fee export from Raisin, as downloaded:
  `Datum;Buchungsart;Betrag in EUR;Bemerkung`, ending with a `Summe` row. Refunds are negative.
  The run checks the `Summe` row against the bookings and stops if they differ.
- **`stocks/source_stocks_riester_value.ods`** — sheet `Wert` with `Datum`, `Wert`, `Kommentar`.
  Add a row whenever you look up the contract value; the newest row is used. Without any row the
  contract is left out of the portfolio (with a warning).
- Contributions come from the `Riester Buy` sheet (see ETFs).

#### Crypto

- **Exchange exports** go into `crypto/` with the file names above. Exports may overlap: repeated
  rows are dropped. An empty export file is skipped.
- **`crypto/source_crypto__master_data.csv`** — `coin;name;symbol;currency`, one row per coin
  ever held; `symbol` is the Yahoo ticker (e.g. `ADA-EUR`), `currency` its quote currency. A coin
  without symbol gets no price (warning).
- Missing or exchange-side bookings: see [Manual transactions](#manual-transactions).

#### Cash: `cash/source_cash_balance_sheet.ods`

| Sheet | Columns | Enter |
| --- | --- | --- |
| `Konten` | `Konto-ID`, `Bank`, `Plattform`, `Name`, `Typ`, `Zinssatz`, `Zinssatz gültig bis`, `Eröffnet`, `Geschlossen`, `Kommentar` | one row per account. `Typ`: `Giro`, `Tagesgeld`, `Verrechnung`, `Sparbuch` (cash) or `Kreditkarte`, `Kredit` (liabilities). `Zinssatz` = current rate (overwrite when it changes). `Plattform` = broker platform (e.g. Raisin) if the money lies with a partner bank. Fill `Geschlossen` instead of deleting a row. |
| `Kontostaende` | `Datum`, `Konto-ID`, `Saldo`, `Kommentar` | the balance of every account, ideally at each month end. Debt as shown by the bank, i.e. **negative** (a positive balance on a credit card or loan stops the run). |
| `Festgeld` | `Vertrags-ID`, `Bank`, `Plattform`, `Name`, `Betrag`, `Zinssatz`, `Beginn`, `Ende`, `Auszahlungskonto`, `Kommentar` | one row per fixed-term deposit. `Auszahlungskonto` = the `Konto-ID` it is paid out to. A renewal is a new row. If interest is capitalized, update `Betrag` after each credit. |

Received interest is **not** entered here but in the `Zinsen` sheet (see ETFs); this workbook only
holds balances, rates and terms.

## How the pipeline works

| Stage | Reads | Writes |
| --- | --- | --- |
| cashflow | Toshl exports, `toshl_exclusion` manual transactions | `transform_cashflow__*` (single and monthly toshl incomes/expenses, the bookings set aside) |
| stocks | trade sheets, mergers, splits, master data, Riester files, Yahoo prices | `transform_etf__*`, `transform_stocks__*`, `transform_portfolio__value.csv`, `transform_portfolio__cashflows.csv` |
| crypto | exchange exports, crypto manual transactions, crypto master data, Yahoo prices | `transform_crypto__*` (ledger, trades, holdings, coins, portfolio value) |
| cash | cash balance sheet | `transform_cash__*` (monthly balances, fixed-term deposits, liquidity) |
| target | the stages above, `toshl_tag_categorization.json`, investment costs | `target_cashflow__*` (all incomes/expenses single and monthly), `target_portfolio__sold_*` |

The dashboard reads `target/` (cashflow, sold positions) and `transform/` (portfolio, crypto,
cash, returns).

## Calculations

All amounts are in EUR. Prices quoted in USD are converted with today's ECB rate (frankfurter.app).

### Incomes and expenses

Every money movement becomes one transaction `[date, tag, category, amount, description]`,
positive for money in, negative for money out. The monthly tables are sums of the very same
transactions, so both always agree.

| Category | Source | Amount |
| --- | --- | --- |
| Toshl categories | Toshl, tag → category via `toshl_tag_categorization.json` | EUR amount; the sign of each entry decides income or expense (a refund on an expense tag is an income) |
| `Investment` | ETF buys/sells and non-holding stock trades | −buy amount, +sell proceeds |
| `order_costs` | the same trades | −order fee of each trade |
| `Investment` (rebalancing) | `Rebalancing` sheet | one row per month: sell proceeds − buy amounts − fees |
| `Dividends` | `Dividends` sheets | each distribution received |
| `Tagesgeld` / `Festgeld` | `Zinsen` sheet | each interest payment received |
| `Riester` | `Riester Buy` sheet | −each contribution |
| `Investment Costs` | `comdirect_costs_combined.csv` | −each cost (a credit becomes positive) |
| `Crypto` | crypto exchange exports | −EUR deposited to an exchange, +EUR withdrawn to the bank |

### Portfolio value (ETFs and stocks)

- **Holdings**: shares per security are summed month by month (buys +, sells −, rebalancing
  included) after mergers and splits; the latest month gives the shares held now. `date` of a
  position is that month (the month of the last trade), not the price date.
- **Value** = shares held × current price (Yahoo, latest close).
- **Invested** (`total_investment`) = sum of buy amounts − sum of sell proceeds of the security,
  **costs** (`total_cost`) = sum of its order fees.
- **Gain** (`value_gained`) = value − invested − costs, `value_gained_pct` = gain / invested.
- **Depot**: the broker(s) holding more than 1 % of the shares, largest first.

### Riester contract

The contract is one more row of the portfolio value (`security_type` `Riester`, no shares or
price):

- **Value** = newest row of `source_stocks_riester_value.ods`.
- **Invested** = sum of all `Riester Buy` contributions; **costs** = sum of the Raisin fee export,
  refunds included.
- **Gain** = value − invested. The fees are *not* subtracted again: Raisin takes them out of the
  contract, so they already lower the value (for ETFs the order fees are paid on top).

### Returns per year (XIRR)

`transform_portfolio__cashflows.csv` lists every cashflow from your point of view — money paid in
negative (buys incl. fees, Riester contributions), money received positive (sell proceeds net of
fees, dividends) — plus the current value of each position as if it were sold today (Riester: on
the date of its value). The dashboard computes from it the money-weighted return per year (XIRR):
the yearly rate at which all these flows add up to zero, so it accounts for *when* money went in
(e.g. monthly savings plans), unlike gain / invested.

### Sold positions

`target_portfolio__sold_positions.csv` lists every sold (closed) ETF and stock position,
`target_portfolio__sold_position_flows.csv` every cashflow of those positions
(`position_id;date;type;amount`, the amounts of a position sum to its net profit).

- A **position** opens with the first buy while nothing of the security is held and closes with
  the sell that takes the holding back to zero (at most 1 % of the most shares held may be left,
  since fractional ETF shares never sum to exactly zero). The rest of a sale spread over several
  orders still belongs to the closed position. Buying the security again opens a new position
  (`<isin>#2`, ...), so a sell-and-rebuy, e.g. to use the tax-free allowance, shows up as a sold
  position flagged `rebought`. Partial sells of positions still held are not listed.
- All trades count, rebalancing included; ETF mergers and stock splits are applied first.
- **Costs**: the order costs of every buy and sell (whatever their sign in the sheets) plus the
  ongoing costs of the per-security cost report (`comdirect_costs_combined.csv`). A yearly cost
  is split over the positions of its security by the days each was held in that year. Costs
  without an ISIN (depot fees, Trade Republic TER/fees) belong to no single position.
- **Dividends** go to the position open on the payment date, or to the one sold last before it.
- `net_profit` = sell proceeds + dividends − buy amount − order costs − ongoing costs,
  `roi_pct` = `net_profit` / buy amount, `annual_return_pct` = money-weighted return per year
  (XIRR) of the position's flows.

### Crypto

All exchange exports (plus the manual transactions) are combined into one ledger
(`transform_crypto__ledger.csv`): one row per balance change of a coin on an exchange or wallet.
From the ledger:

- `transform_crypto__trades.csv` — every trade with the EUR cost basis it moved and its fees in EUR.
- `transform_crypto__holdings_monthly.csv` — cumulative amount per exchange/wallet and coin per month.
- `transform_crypto__coins.csv` — one row per coin ever held: amount, current EUR price and value,
  `total_investment` (cost basis of the coins held), `total_cost` (fees), unrealized
  `value_gained` and `realized_gain` of sales for EUR.
- `transform_crypto__portfolio_value.csv` — the same per exchange/wallet for the coins held now
  (same value/gain columns as the ETF/stock portfolio value).

The cost basis uses the **average-cost method** over all exchanges: a crypto→crypto swap carries
the EUR cost of the coins sold over to the coins bought, airdrops (Kraken `transfer`) come in at
zero cost, moves between own exchanges/wallets keep their cost, and every fee is valued at the
average cost of the coin it is paid in and booked as cost of the coin traded or moved. Coins that
left with no known counterpart (`removal`) are taken out at average cost without inventing
proceeds or a gain/loss; that cost is shown as `removed_cost` in `transform_crypto__coins.csv`.

**Delisting conversions**: when an exchange stops supporting a coin, it converts the remaining
balance into another coin. Kraken books both sides as two unrelated `transfer` rows with
different refids and dates, which would otherwise look like coins vanishing and an airdrop
appearing. Such pairs are marked in the manual transactions (`crypto_conversion`) and booked as
one trade, so the cost basis of the delisted coin moves on to the coin received.

### Cash

No interest amounts are calculated (received interest comes from the `Zinsen` sheet); rates are
passed on for display.

- `transform_cash__balances_monthly.csv` — the balance of every account at each month end, from
  its first balance up to the current month. A month without its own balance carries the latest
  one forward (`balance_date` says which), flagged `is_stale` when that balance is more than 35
  days old; months after `Geschlossen` are dropped. `is_liability` marks credit cards and loans.
- `transform_cash__term_deposits.csv` — per fixed-term deposit as of today: `status` (`läuft`
  from `Beginn` until the day before `Ende`, `abgelaufen` from `Ende` on — the payout day —,
  `zukünftig` before `Beginn`), `term_days` = `Ende` − `Beginn`, `days_remaining`, and
  `progress_pct` = elapsed / term.
- `transform_cash__liquidity_monthly.csv` — per month end: `verfügbar` = sum of the cash account
  balances, `gebunden` = sum of the deposits running at that month end, `verbindlichkeiten` = sum
  of the credit card and loan balances (negative). After the current month it is a projection
  (`is_projection`) until the last maturity: balances and liabilities stay as they are and each
  maturing deposit moves its `Betrag` from `gebunden` to `verfügbar`. Months before the first
  recorded balance only have `gebunden`.

## Manual transactions

Every manual correction of the source data lives in the single file
`source/source_manual__transactions.csv` — the code never contains concrete bookings. Columns
(`;` separated, `,` decimals): `action;date;exchange;type;coin;amount;fee;group;refid;tag;description;comment`.
The `action` decides which columns are used; unknown actions fail the pipeline. Every row needs a
`comment` saying where its numbers come from. Nothing is estimated: only confirmed balances and
bookings found in other sources are entered.

| Action | Purpose | Columns used |
| --- | --- | --- |
| `crypto_transaction` | Coin movement missing in the exports (e.g. a transfer to a private wallet, a balance confirmed after the last export). `type` is `deposit`, `withdrawal`, `trade`, `reward` or `removal` (coins gone with no known counterpart). | `date` (UTC), `exchange`, `type`, `coin`, `amount`, `fee`; legs of one manual trade share a `group` |
| `crypto_conversion` | One side of a conversion done by the exchange (e.g. on delisting), referencing the exported booking. | `exchange`, `refid` of the exported booking; both sides share a `group` |
| `toshl_exclusion` | Toshl booking that another source already books (a double booking), set aside with the `comment` as reason. An entry matching no or several toshl bookings is logged as a warning. | `date` (YYYY-MM-DD), `tag`, `amount` (positive, as shown in toshl), `description` |

## Special cases

- **Vacation**: any Toshl entry with category or tag containing `Urlaub` is remapped to a
  `vacation` tag.
- **Rebalancing**: rebalancing trades move real shares (so they count towards holdings and cost
  basis), but only their monthly *net* is booked as cashflow — folded into the `Investment`
  income/expense column, never a separate category.
- **Non-portfolio stock trades** (e.g. an expired put option): booked as an investment expense
  rather than a holding.
- **Cost credits**: a negative cost in the investment-cost report of comdirect in 2020 (e.g. for
  Gold) becomes a cost reduction. Cost months absent from the cashflow table are dropped (logged
  as a warning).
- **Enrichment columns** (Investment, Dividends, interest, Riester, Investment Costs, rebalancing)
  are added *after* the tag→category step and are intentionally outside the toshl category mapping.
- **Stocks carry no ISIN** and are identified by name; ETFs are identified by ISIN.
- **Crypto cashflow** is the EUR moved between bank and exchanges, not the single trades. Toshl
  bookings in the toshl category `Crypto` are therefore set aside (saved in
  `transform_cashflow__toshl_replaced_bookings.csv`) so they are not counted twice.
- **Excluded toshl bookings**: single toshl bookings that another source already contains are
  set aside via `toshl_exclusion` manual transactions, with their reason in
  `transform_cashflow__toshl_replaced_bookings.csv`.
- **KuCoin exports** are in UTC+8 and converted to UTC; all other exports are in UTC.
