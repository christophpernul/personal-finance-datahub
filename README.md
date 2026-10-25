# personal-finance-datahub
Provides a datahub for the following data sources:
- cashflow
- ETF portfolio
- ETF and stocks
- crypto currencies
- consumption

The data is stored inside a `datahub` directory, where it's location can be configured.

```
datahub/
│
├── source/
│   ├── cashflow/
│   │   ├── toshl/
│   │   └── userinput/
│   ├── consumption/
│   ├── crypto/
│   └── stocks/
│
└── target/
    ├── cashflow/
    ├── crypto/
    └── stocks/
```

## Data processing

The datahub pipeline (`run.py`) runs four stages — cashflow → stocks → crypto → target — and
produces the dashboard-ready tables: **incomes**, **expenses**, **portfolio value** and
**crypto portfolio value**.

### Data sources

| Source | File | Feeds |
| --- | --- | --- |
| Toshl exports | `bilanz_*.csv` | incomes & expenses (by tag) |
| ETF trades | `source_stocks_portfolio_trades.ods` (sheets: Buys, Sells, Rebalancing, Dividends, Zinsen, Riester Buy) | portfolio, investment/dividend/interest income, investment & Riester expenses |
| Single-stock trades | `source_stocks_trades.ods` (Buys, Sells, Dividends) | portfolio, investment income/expenses, dividends |
| Investment costs | `comdirect_costs_combined.csv` (comdirect cost reports + Trade Republic fees) | expenses |
| Master data / prices | ETF & stock master data, mergers, splits, yahoo prices | portfolio valuation |
| Crypto exchange exports | `source/crypto/`: `kraken_ledgers_*.csv`, `binance_tradehistory_*.csv`, `binance_deposits_*.xlsx`, `kucoin_trades_*.csv`, `kucoin_deposits_*.csv` | crypto portfolio, `Crypto` income/expenses |
| Crypto master data | `source_crypto__master_data.csv` (coin → yahoo symbol) | crypto portfolio valuation |
| Manual transactions | `source_manual__transactions.csv` (every manual correction of the source data, see below) | crypto portfolio, incomes & expenses |

### Incomes

- **Toshl income tags** grouped into custom categories via `toshl_tag_categorization.json`.
- **Investment** — ETF/stock sell proceeds (plus the toshl `Investment Profit` tag).
- **Dividends** — received ETF distributions and stock dividends.
- **Tagesgeld / Festgeld** — interest on instant-access and fixed-term cash deposits.
- **Crypto** — EUR withdrawn from a crypto exchange back to the bank.

### Expenses (stored as negative amounts)

- **Toshl expense tags** grouped into custom categories.
- **Investment** — ETF/stock buys. Trade `order_costs` are added as their own separate column.
- **Investment Costs** — comdirect Wertpapierkosten/Depotentgelt + Trade Republic fees.
- **Riester** — monthly and one-off Riester pension contributions.
- **Crypto** — EUR deposited from the bank to a crypto exchange.

### Portfolio value

Cumulative monthly share holdings × current prices, combined into one table with a
`security_type` of `ETF` or `Aktie`.

### Crypto

All exchange exports (plus the manual transactions) are combined into one ledger
(`transform_crypto__ledger.csv`): one row per balance change of a coin on an exchange or wallet.
New exports only need to be dropped into `source/crypto/`; rows repeated by overlapping exports
are dropped. From the ledger:

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

### Manual transactions

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

### Exceptions

- **Vacation**: any entry with category or tag containing `Urlaub` is remapped to a `vacation` tag.
- **Rebalancing**: rebalancing trades move real shares (so they count towards holdings and cost
  basis), but only their monthly *net* is booked as cashflow — folded into the `Investment`
  income/expense column, never a separate category.
- **Non-portfolio stock trades** (e.g. an expired put option): booked as an investment expense
  rather than a holding.
- **Cost credits**: a negative cost in the investment-cost report of comdirect in 2020 (e.g. for Gold) becomes a
  cost reduction. Cost months absent from the cashflow table are dropped (logged as a warning).
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

## Usage

### Cashflow
