import yfinance as yf
import polars as pl
from equity_list import equities

def get_stock_data(equities):
    return yf.download(tickers=list(equities.keys()), period="10y", group_by="ticker")

# stock_data = get_stock_data(equities)
# print(stock_data)