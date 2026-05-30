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

## Usage

### Cashflow

Running `cashflow_main.py` loads all _toshl_ exports, preprocesses them, and converts the cashflow data into
income and expenses using custom categorization.
