import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """loads data into staging tables

    Args:
        cur: psycopg2 cursor
        conn: psycopg2 connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """inserts data into fact and dimension tables from staging

    Args:
        cur: psycopg2 cursor
        conn: psycopg2 connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    cluster_config = config['CLUSTER']
    conn = psycopg2.connect(f"host={cluster_config.get('HOST')} \
     dbname={cluster_config.get('DB_NAME')} \
     user={cluster_config.get('DB_USER')} \
     password={cluster_config.get('DB_PASSWORD')} \
     port={cluster_config.get('DB_PORT')}")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()