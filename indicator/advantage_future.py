import pandas_ta as ta
import numpy as np


def calculate(df, length=60):
    df.reset_index(inplace=True)
    df['bull_ae'] = np.NAN
    df['bear_ae'] = np.NAN
    df['bull_mfe'] = np.NAN
    df['bull_mae'] = np.NAN
    df['bear_mfe'] = np.NAN
    df['bear_mae'] = np.NAN

    for index, row in df.iloc[length:df.index[-1] - length].iterrows():
        high = df['high'].iloc[index:index + length].max()
        low = df['low'].iloc[index:index + length].min()

        bull_mfe = high - row['high']
        bull_mae = row['high'] - low
        bear_mfe = row['low'] - low
        bear_mae = high - row['low']

        df.loc[index, 'bull_ae'] = bull_mfe / bull_mae if bull_mae != 0 else 99
        df.loc[index, 'bear_ae'] = bear_mfe / bear_mae if bear_mae != 0 else 99

        df.loc[index, 'bull_mfe'] = bull_mfe
        df.loc[index, 'bull_mae'] = bull_mae
        df.loc[index, 'bear_mfe'] = bear_mfe
        df.loc[index, 'bear_mae'] = bear_mae
        df.loc[index, 'stop_high'] = df['high'].iloc[index - length:index].max()
        df.loc[index, 'stop_low'] = df['low'].iloc[index - length:index].min()

    df.set_index('date', inplace=True)

    return df
