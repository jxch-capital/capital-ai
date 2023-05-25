from config import config
import datasource.raw_k_db as kdb

try:
    # kdb.raw_k2db()
    # kdb.table_index()
    print(kdb.find_by_code_and_interval('SPY', '1d'))
finally:
    config.close()
