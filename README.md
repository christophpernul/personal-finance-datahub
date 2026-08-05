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

The datahub pipeline (`run.py`) runs three stages — cashflow → stocks → target — and produces three
dashboard-ready target tables: **incomes**, **expenses** and **portfolio value**.

### Data sources

| Source | File | Feeds |
| --- | --- | --- |
| Toshl exports | `bilanz_*.csv` | incomes & expenses (by tag) |
| ETF trades | `source_stocks_portfolio_trades.ods` (sheets: Buys, Sells, Rebalancing, Dividends, Zinsen, Riester Buy) | portfolio, investment/dividend/interest income, investment & Riester expenses |
| Single-stock trades | `source_stocks_trades.ods` (Buys, Sells, Dividends) | portfolio, investment income/expenses, dividends |
| Investment costs | `comdirect_costs_combined.csv` (comdirect cost reports + Trade Republic fees) | expenses |
| Master data / prices | ETF & stock master data, mergers, splits, yahoo prices | portfolio valuation |

### Incomes

- **Toshl income tags** grouped into custom categories via `toshl_tag_categorization.json`.
- **Investment** — ETF/stock sell proceeds (plus the toshl `Investment Profit` tag).
- **Dividends** — received ETF distributions and stock dividends.
- **Tagesgeld / Festgeld** — interest on instant-access and fixed-term cash deposits.

### Expenses (stored as negative amounts)

- **Toshl expense tags** grouped into custom categories.
- **Investment** — ETF/stock buys. Trade `order_costs` are added as their own separate column.
- **Investment Costs** — comdirect Wertpapierkosten/Depotentgelt + Trade Republic fees.
- **Riester** — monthly and one-off Riester pension contributions.

### Portfolio value

Cumulative monthly share holdings × current prices, combined into one table with a
`security_type` of `ETF` or `Aktie`.

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

## Usage

### Cashflow
