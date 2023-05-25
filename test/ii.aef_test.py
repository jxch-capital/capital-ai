import indicator.advantage_future as aef
import datasource.raw_k_db as kdb
from config import config

try:
    df = kdb.find_by_code_and_interval('QQQ', '1d')
    ae = aef.calculate(df)
    print(ae)
finally:
    config.close()
