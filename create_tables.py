import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()