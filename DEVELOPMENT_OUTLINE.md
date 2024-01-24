# Architecture Overview

We aim to split the application part from the data processing.
There should be a single application module, that only serves the
application by loading already preprocessed data. Furthermore the data
preprocessing is split into the following stages:

- extract
  - Extracts data from source systems, e.g. toshl or websites
  - This data is stored in the `datahub/source/` directory
- transform
  - Performs preprocessing of the source data, e.g. data cleaning
  - This data is stored in the `datahub/transform` directory
- application
  - Converts data to a format, where it can easily be used from the
    application itself.
  - This data is stored inside the `datahub/application` directory

With this design we save the data in different stages, which allows easier
debugging and each stage can be executed independently.

## Datahub folder structure

Each folder can contain sub folders for different data domains. Example
domains are `etf`, `stocks` and `cashflow`. Each domain can consist of
sub folders for different data sources. Source data, that is not extracted
from another system, but provided manually can be stored in a
`datahub/source/{domain}/userinput` directory.

The proposed directory structure of the datahub looks like this:

```
datahub/
│
├── application/
│   ├── cashflow/
│   ├── crypto/
│   ├── etf/
│   └── stocks/
│
├── source/
│   ├── cashflow/
│   │   ├── toshl/
│   │   └── userinput/
│   ├── crypto/
│   ├── etf/
│   └── stocks/
│
└── transform/
    ├── cashflow/
    ├── crypto/
    ├── etf/
    └── stocks/
```

## Datahub execution structure

For each domain we create an own datahub module inside the
`src/` directory.

```
src/
│
├── datahub_crypto/
│   ├── __init__.py
│   ├── extract
│   │   ├── __init__.py
│   │   └── extract_crypto_data.py
│   └── transform
│       ├── __init__.py
│       └── transform_crypto_data.py
│
├── datahub_etfs/
│   ├── __init__.py
│   ├── extract
│   │   ├── __init__.py
│   │   └── extract_etf_data.py
│   └── transform
│       ├── __init__.py
│       └── transform_etf_data.py
│
├── datahub_stocks/
│   ├── __init__.py
│   ├── extract
│   │   ├── __init__.py
│   │   └── extract_stock_data.py
│   └── transform
│       ├── __init__.py
│       └── transform_stock_data.py
│
└── datahub_cashflow/
│   ├── __init__.py
│   ├── extract
│   │   ├── __init__.py
│   │   └── extract_cashflow_data.py
│   └── transform
│       ├── __init__.py
│       └── transform_cashflow_data.py

```
