from config import config


def create_schema():
    with config.get_db_conn() as conn:
        cur = conn.cursor()
        cur.execute(f'create schema {config.db_schema}')
        conn.commit()
