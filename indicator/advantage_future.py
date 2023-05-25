import pandas_ta as ta
import numpy as np


def calculate(df, length=60, atr_length=14):
    df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=atr_length)

    df.reset_index(inplace=True)
    df['bull_ae'] = np.NAN
    df['bear_ae'] = np.NAN

    for index, row in df.iloc[atr_length:df.index[-1] - length].iterrows():
        high = df['high'].iloc[index:index + length].max()
        low = df['low'].iloc[index:index + length].min()
        atr = row['atr']

        bull_mfe = (high - row['high']) / atr
        bull_mae = (row['high'] - low) / atr
        bear_mfe = (row['low'] - low) / atr
        bear_mae = (high - row['low']) / atr

        df.loc[index, 'bull_ae'] = bull_mfe / bull_mae if bull_mae != 0 else 99
        df.loc[index, 'bear_ae'] = bear_mfe / bear_mae if bear_mae != 0 else 99

    df.set_index('date', inplace=True)

    return df
