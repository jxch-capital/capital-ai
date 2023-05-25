import datasource.codes_db as cdb
from config import config

try:
    print(cdb.init())
finally:
    config.close()
