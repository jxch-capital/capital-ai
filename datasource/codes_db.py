import datasource.yahoo as yahoo
from config import config
import pandas as pd

table_name = 'download_codes'


def codes2db(if_exists='replace'):
    yahoo.codes_df().to_sql(table_name, config.get_pd2db_engine(), schema=config.db_schema,
                            if_exists=if_exists, index=False)


def find_all():
    return pd.read_sql(f'select * from {config.db_schema}.{table_name}', con=config.get_pd2db_engine())


def find_all_code():
    return [code for _, row in find_all().iterrows() for code in row['codes'].split(',')]


def table_index():
    with config.get_db_conn() as conn, conn.cursor() as cur:
        cur.execute(f'alter table {config.db_schema}.{table_name} add id bigserial')
        cur.execute(f'alter table {config.db_schema}.{table_name} add primary key(id)')
        cur.execute(f'create index download_codes_type_engine_index on {config.db_schema}.{table_name} (type, engine)')
        conn.commit()


def init():
    codes2db()
    table_index()
    return find_all_code()
