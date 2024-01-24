import investpy

# List of ETF ISINs
etf_isins = [
    "IE00BFNM3P36",
    "IE00BL25JM42",
    "IE00B4ND3602",
]  # Replace with your actual ISINs

import yfinance as yf

msft = yf.Ticker("AYEM.DE")

# get all stock info
msft.info
