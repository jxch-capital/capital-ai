import yfinance as yf
import pandas as pd
import numpy as np

engine = 'yfinance'
intervals_d = ['1d', '5d', '1wk', '1mo', '3mo']
intervals_m = ['5m', '15m', '30m', '60m', '90m']
codes = {
    'us-it': 'QQQ,GOOGL,AAPL,MSFT,TSLA,AMZN,AMD,TSM,NVDA,META,INTC,IBM,ORCL,HPQ',
    'us-industry': 'SPY,XLI,XLE,XLY,XLP,XLF,XLV,XLC,XLB,XLRE,XLK,XLU',
    'us-industry-detailed': 'XHB,XRT,XLP,XOP,OIH,TAN,URA,KBE,KIE,IAI,IBB,IHI,IHF,XPH,ITA,IYT,JETS,GDX,XME,LIT,REMX,IYM,'
                            'VNQ,VNQI,REM,VGT,FDN,SOCL,IGV,SOXX',
    'us-market': 'VTI,DIA,OEF,MDY,IWB,IWM,QTEC,IVW,IVE,IWF,IWD',
    'us-theme': 'MOAT,FFTY,IBUY,CIBR,SKYY,IPAY,FINX,XT,ARKK,BOTZ,MOO,ARKG,MJ,ARKW,ARKQ,PBW,BLOK,SNSR',
    'us-blue': 'BRK-B,BA,CHKP,MRK,JPM,MS,T,JNPR,MCD,ORCL,DUK,GE,KO,XOM,AEP,PEP,JNJ,ADBE,WFC,ERIC,C,CPB,BAC,'
               'HPQ,PFE,CSCO,F,XRX,GM,DAL,DIS,AMAT,UNH,WMT,PG,MA,NKE,FDX,MMM',
    'us-futures': 'ES=F,NQ=F,YM=F,RTY=F,GC=F,HG=F,CL=F,ZC=F,ZW=F,KC=F,SB=F,CC=F,BTC=F,LE=F,CT=F,ZS=F,PA=F,SI=F,PL=F',
    'us-china': 'BABA,BIDU,JD,PDD,NTES,WB,BILI,TCOM',
    'china-industry': '000001.SS,000032.SZ,000034.SZ,000035.SZ,000036.SZ,000037.SZ,000038.SZ,000039.SZ,000040.SZ,'
                      '000042.SZ',
    'china-blue': '600519.SS,600600.SS,601318.SS,600887.SS,603288.SS,000538.SZ,000333.SZ,002594.SZ,601607.SS,000002.SZ',
}


def codes_df():
    return pd.DataFrame([{'engine': engine, 'type': key, 'codes': val} for key, val in codes.items()])


def download_by_codes(codes_str, intervals_str=','.join(intervals_d), period='max'):
    return {interval: yf.download(codes_str, interval=interval, period=period) for interval in intervals_str.split(",")}


def df_arr2df(interval_df):
    dfs = []
    for interval, df in interval_df.items():
        for col_name in df['Close'].columns.values:
            tmp_df = pd.DataFrame({
                'adj_close': df['Adj Close'][col_name],
                'close': df['Close'][col_name],
                'high': df['High'][col_name],
                'low': df['Low'][col_name],
                'open': df['Open'][col_name],
                'volume': df['Volume'][col_name],
                'interval': interval,
                'date': df.index,
                'code': col_name
            })
            tmp_df.dropna(axis=0, how='any', inplace=True)
            tmp_df = tmp_df[~tmp_df.index.duplicated(keep='first')]
            tmp_df = tmp_df.loc[tmp_df['low'].gt(10).idxmin():tmp_df.index[-1]] if \
                tmp_df['low'].iloc[0] < 10 else tmp_df

            tmp_df.reset_index(inplace=True, drop=True)
            dfs.append(tmp_df)

    return pd.concat(dfs, ignore_index=True)
