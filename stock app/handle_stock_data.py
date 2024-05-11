import polars as pl

def handle_stock_data_prices(data, equities):

    dataframes = []

    for ticker, _ in equities.items():

        ticker_data = pl.from_pandas(data[ticker].reset_index())
        ticker_data = ticker_data.select([
            pl.col('Date').dt.date().alias('Date'),
            pl.col('Close').cast(pl.Float32).alias(f"{ticker}_Close")
        ])
        dataframes.append(ticker_data)
    stock_data_df = dataframes[0]
    
    for dataframe in dataframes[1:]:
        stock_data_df = stock_data_df.join(dataframe, on='Date')

    return stock_data_df