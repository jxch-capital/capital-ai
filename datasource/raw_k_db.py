import datasource.yahoo as yahoo
import datasource.codes_db as codes_db
from config import config
import pandas as pd
import logging

table_name = 'raw_k'


def raw_k2db():
    df = codes_db.find_all()
    for index, row in df.iterrows():
        if yahoo.engine == row['engine']:
            df_arr = yahoo.download_by_codes(row['codes'])
            all_df = yahoo.df_arr2df(df_arr)
            all_df.to_sql(table_name, config.get_pd2db_engine(), schema=config.db_schema,
                          if_exists='append', index=False)
            logging.info(f'---->> {index + 1}/{df.shape[0]} {row["codes"]}')


def table_index():
    with config.get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(f'alter table {config.db_schema}.{table_name} add id bigserial')
        cur.execute(f'alter table {config.db_schema}.{table_name} add primary key(id)')
        cur.execute(f'create index "raw_k_Code_interval_K_Index_Date_index" on {config.db_schema}.{table_name} (code, interval, date)')
        conn.commit()


def find_all():
    return pd.read_sql(f'select * from {config.db_schema}.{table_name}', con=config.get_pd2db_engine())


def find_all_by_code(code_arr):
    code_arr = list(map(lambda x: f"'{x}'", code_arr))
    return pd.read_sql(f"select * from {config.db_schema}.{table_name} where code in ({','.join(code_arr)})",con=config.get_pd2db_engine())


def find_all_by_interval(interval):
    return pd.read_sql(f"select * from {config.db_schema}.{table_name} where interval='{interval}'",con=config.get_pd2db_engine())


def find_by_code_and_interval(code, interval):
    df = pd.read_sql(f"select * from {config.db_schema}.{table_name} where code='{code}' and interval='{interval}'",
                     con=config.get_pd2db_engine())
    df.set_index('date', inplace=True)
    return df
