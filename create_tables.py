import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, staging_events_copy, staging_events_table_create, \
    staging_songs_table_create, staging_songs_copy


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
    conn = psycopg2.connect(f"host={cluster_config.get('DB_HOST')} \
     dbname={cluster_config.get('DB_NAME')} \
     user={cluster_config.get('DB_USER')} \
     password={cluster_config.get('DB_PASSWORD')} \
     port={cluster_config.get('DB_PORT')}")
    cur = conn.cursor()

    # Create log events staging table
    print(staging_events_table_create)
    cur.execute(staging_events_table_create)
    conn.commit()

    # Populate log events staging table
    print(staging_events_copy)
    cur.execute(staging_events_copy)
    conn.commit()

    # Create song data staging table
    print(staging_songs_table_create)
    cur.execute(staging_songs_table_create)
    conn.commit()

    # Populate song data staging table
    print(staging_songs_copy)
    cur.execute(staging_songs_copy)
    conn.commit()

    # drop_tables(cur, conn)
    # create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()