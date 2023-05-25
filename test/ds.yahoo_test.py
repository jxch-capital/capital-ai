import datasource.yahoo as yh
from config import config

idf = yh.download_by_codes('QQQ,SPY')

df = yh.df_arr2df(idf)

print(df)
