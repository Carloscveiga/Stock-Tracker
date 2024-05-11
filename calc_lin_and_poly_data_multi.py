import polars as pl
from scipy.stats import norm
import numpy as np


def calc_lin_and_poly_data_multi(data, start_period, end_period):
    
    sliced_data = data[start_period:end_period]
    sliced_data = sliced_data.with_columns(pl.arange(0, sliced_data.height).alias('x'))
    stock_results = {}
    
    for name in sliced_data.columns:
        if "_Close" in name:
            ticker = name.split("_")[0]
            n = sliced_data.height
            sum_x = sliced_data['x'].sum()
            sum_x2 = (sliced_data['x'] ** 2).sum()
            sum_y = sliced_data[name].sum()
            sum_xy = (sliced_data['x'] * sliced_data[name]).sum()
            denominator = (n * sum_x2 - sum_x ** 2)
            if denominator == 0:
                continue  
            
            b = (n * sum_xy - sum_x * sum_y) / denominator
            a = (sum_y - b * sum_x) / n
            lin_trendline = a + b * sliced_data['x']
            lin_trendline_np = lin_trendline.cast(pl.Float32)            
            residuals = sliced_data[name] - lin_trendline
            avg_residuals = residuals.mean()
            std_residuals = residuals.std()
            if avg_residuals is None or std_residuals is None:
                
                continue
            y_mean = sliced_data[name].mean()
            sst = ((sliced_data[name] - y_mean) ** 2).sum()
            ssr = (residuals ** 2).sum()
            lin_r_squared = 1 - (ssr / sst)
            lin_position_values = norm.cdf(residuals, loc=avg_residuals, scale=std_residuals)
            
                    
            x = sliced_data['x'].to_numpy()
            y = sliced_data[name].to_numpy()
            degree = 2
            coefficients = np.polyfit(x, y, degree)
            trendline_poly = np.polyval(coefficients, x)
            residuals_poly = y - trendline_poly
            avg_residuals_poly = residuals_poly.mean()
            std_dev_residuals_poly = residuals_poly.std()
            if avg_residuals_poly is None or std_dev_residuals_poly is None:
                continue
            
            y_mean = np.mean(y)
            sst = np.sum((y - y_mean) ** 2)
            ssr = np.sum(residuals_poly ** 2)
            r_squared_poly = 1 - (ssr / sst)
            position_values_poly = norm.cdf(residuals_poly, loc=avg_residuals_poly, scale=std_dev_residuals_poly)
            
            lin_r_value_series = pl.Series(f"{ticker}_Lin_R_value", [lin_r_squared] * sliced_data.height, dtype=pl.Float32)
            poly_r_value_series = pl.Series(f"{ticker}_Poly_R_value", [r_squared_poly] * sliced_data.height, dtype=pl.Float32)
            
            lin_trendline_series = pl.Series(f"{ticker}_Lin_Trendline", lin_trendline_np, dtype=pl.Float32)
            lin_position_values_series = pl.Series(f"{ticker}_Lin Position Values", lin_position_values, dtype=pl.Float32)
            poly_trendline_series = pl.Series(f"{ticker}_Poly Trendline", trendline_poly, dtype=pl.Float32)
            position_values_poly_series = pl.Series(f"{ticker}_Poly Position Values", position_values_poly, dtype=pl.Float32)

            sliced_data = sliced_data.with_columns([
                lin_r_value_series,
                lin_trendline_series,
                lin_position_values_series,
                poly_r_value_series,
                poly_trendline_series,
                position_values_poly_series,
            ])

    linreg_df = sliced_data.with_columns(sliced_data)
    
    
    return linreg_df 



def calc_lin_and_poly_signal_data_multi(data, min_r_value):
    buy_threshold = 0.05 / 2
    sell_threshold = 1 - (0.05 / 2)
    signals_df = pl.DataFrame()

    for column in data.columns:
        if "_Close" in column:
            
            ticker = column.split("_Close")[0]            
            close_column = f"{ticker}_Close"
            lin_trendline_column = f"{ticker}_Lin_Trendline"
            lin_positional_values_column = f"{ticker}_Lin Position Values"
            poly_trendline_column = f"{ticker}_Poly Trendline"
            poly_positional_values_column = f"{ticker}_Poly Position Values"
            lin_r_value_collumn = f"{ticker}_Lin_R_value"
            poly_r_value_collumn = f"{ticker}_Poly_R_value"

            missing_columns = []
            for required_column in [close_column, lin_trendline_column, lin_positional_values_column, 
                                    poly_trendline_column, poly_positional_values_column]:
                if required_column not in data.columns:
                    missing_columns.append(pl.lit(0).alias(required_column))

            if missing_columns:
                data = data.with_columns(missing_columns)

            # Calculate the linear and polynomial signals
            lin_signal = pl.when(data[lin_positional_values_column] <= buy_threshold).then(1) \
                            .when(data[lin_positional_values_column] >= sell_threshold).then(-1) \
                            .otherwise(0).alias(f"{ticker}_Lin_Signal")
                            
            lin_signal_with_r_value = pl.when((lin_signal == 1) & (data[lin_r_value_collumn] >= min_r_value)).then(1) \
                                        .when((lin_signal == -1) & (data[lin_r_value_collumn] >= min_r_value)).then(-1) \
                                        .otherwise(0).alias(f"{ticker}_Lin_Signal_With_R")
                
            poly_signal = pl.when(data[poly_positional_values_column] <= buy_threshold).then(1) \
                            .when(data[poly_positional_values_column] >= sell_threshold).then(-1) \
                            .otherwise(0).alias(f"{ticker}_Poly_Signal")
                            
            poly_signal_with_r_value = pl.when((poly_signal == 1) & (data[poly_r_value_collumn] >= min_r_value)).then(1) \
                                        .when((poly_signal == -1) & (data[poly_r_value_collumn] >= min_r_value)).then(-1) \
                                        .otherwise(0).alias(f"{ticker}_Poly_Signal_With_R")
                            
            previous_close = data[close_column].shift(1)
            previous_lin_trendline = data[lin_trendline_column].shift(1)
            previous_poly_trendline = data[poly_trendline_column].shift(1)

            # Detect crossovers for linear trendline
            lin_crossover = (data[close_column] - data[lin_trendline_column]) * (previous_close - previous_lin_trendline) < 0
            lin_crossover_signal = lin_crossover.cast(pl.Int8).alias(f"{ticker}_Lin_Crossover")

            # Detect crossovers for polynomial trendline
            poly_crossover = (data[close_column] - data[poly_trendline_column]) * (previous_close - previous_poly_trendline) < 0
            poly_crossover_signal = poly_crossover.cast(pl.Int8).alias(f"{ticker}_Poly_Crossover")

            # Append these columns to the signals DataFrame
            signals_df = signals_df.with_columns([
                data['Date'],
                data[close_column],
                data[lin_trendline_column],
                lin_crossover_signal,
                data[lin_positional_values_column],
                lin_signal.cast(pl.Int8),
                lin_signal_with_r_value.cast(pl.Int8),
                data[poly_trendline_column],
                poly_crossover_signal,
                data[poly_positional_values_column],
                poly_signal.cast(pl.Int8),
                poly_signal_with_r_value.cast(pl.Int8),
            ])

    return signals_df


