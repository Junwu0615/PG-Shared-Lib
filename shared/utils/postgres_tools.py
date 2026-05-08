# -*- coding: utf-8 -*-
"""
TODO
    Update Date: 2026-04-04
    Notice:
        - Session 設定
            - SET synchronous_commit = OFF;
                - [真實業務保持] ON  : 等待資料寫入磁碟後才回應，確保資料安全，但恐增加延遲
                - [壓測必開]    OFF : 不等待資料寫入磁碟就回應，提升性能，但在系統崩潰時可能會丟失最近的交易
"""
from shared.configs import psycopg2


def get_conn(db, logging=None) -> psycopg2.extensions.connection:
    """建立 Postgresql 連線"""
    while True:
        try:
            conn = psycopg2.connect(**db)
            conn.autocommit = False

            # 連線建立後 立刻執行 Session 等級的設定
            with conn.cursor() as cur:
                cur.execute('SET synchronous_commit = OFF;')

            return conn

        except Exception as e:
            if logging is not None:
                logging.error('Connect Failed Retrying...', exc_info=True)
                time.sleep(3)


def close_conn(conn, cursor, logging=None):
    """安全關閉 Postgresql 連線"""
    if cursor:
        cursor.close()
        if logging is not None:
            logging.notice('<cursor.close()> called ...')
    if conn:
        conn.close()
        if logging is not None:
            logging.notice('<conn.close()> called ...')


def table_exists(cursor, schema_name, table_name):
    """檢查指定的 Schema 和 Table 是否存在"""
    query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = %s
            AND    table_name   = %s
        );
    """
    cursor.execute(query, (schema_name, table_name))
    return cursor.fetchone()[0]