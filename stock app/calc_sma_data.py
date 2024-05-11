

def calc_sma_data(data, start_period, end_period):

    sliced_data = data[start_period:end_period]
    sma_columns = []

    for name in sliced_data.columns:
        if "_Close" in name:
            ticker = name.split("_")[0]
            close_column = sliced_data[name]
            sma_columns.extend([
                close_column.rolling_mean(window_size=5).alias(f"{ticker}_SMA_5"),
                close_column.rolling_mean(window_size=15).alias(f"{ticker}_SMA_15"),
                close_column.rolling_mean(window_size=20).alias(f"{ticker}_SMA_20"),
                close_column.rolling_mean(window_size=200).alias(f"{ticker}_SMA_200"),
            ])
       
    all_columns = sma_columns 
    sma_linreg_df = sliced_data.with_columns(all_columns)
    last_row_df = sma_linreg_df.tail(1200)

    return last_row_df