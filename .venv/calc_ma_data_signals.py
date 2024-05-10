import polars as pl
# from calc_sma_data import calc_sma_data
# from equitiy_list import equities
# from get_stock_data import get_stock_data
# from handle_stock_data import handle_stock_data_prices
import numpy as np

def calc_sma_signal_data(data):
 
    signals_df = pl.DataFrame({'Date': data['Date']})

    for column in data.columns:
        
        if "_SMA_5" in column:
            ticker = column.split("_SMA_5")[0]
            close_collumn = f"{ticker}_Close"
            sma5_column = f"{ticker}_SMA_5"
            sma20_column = f"{ticker}_SMA_20"
            sma200_column = f"{ticker}_SMA_200"
     
            sma5_gt_sma20 = data[sma5_column] > data[sma20_column]
            previous_sma5_gt_sma20 = sma5_gt_sma20.shift(1).fill_null(False)
            
            bullish_cross = (~previous_sma5_gt_sma20) & sma5_gt_sma20
            bullish_cross[0] = False
            bearish_cross = previous_sma5_gt_sma20 & (~sma5_gt_sma20)
            bearish_cross[0] = False

            sma200_diff = data[sma200_column].diff()
            sma200_trend = (sma200_diff > 0).cast(pl.Int8) - (sma200_diff < 0).cast(pl.Int8)
            
            buy_condition = bullish_cross & (sma200_trend == 1 )
            sell_condition = bearish_cross & (sma200_trend == -1)
            
            signal = pl.when(buy_condition).then(1) \
                            .when(sell_condition).then(-1) \
                            .otherwise(0).alias(f"{ticker}_Signal")
            
            signals_df = signals_df.with_columns([
                data[close_collumn],
                pl.Series(f"{ticker}_Bullish_Cross", bullish_cross).cast(pl.Int8),
                pl.Series(f"{ticker}_Bearish_Cross", bearish_cross).cast(pl.Int8),
                pl.Series(f"{ticker}_SMA200_Trend", sma200_trend).cast(pl.Int8),
                signal
            ])
            
    return signals_df


