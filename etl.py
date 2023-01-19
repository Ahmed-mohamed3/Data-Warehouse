import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')



def load_staging_tables(cur, conn):
    """
    data gets loaded from S3 into staging tables on Redshift
    - cur: database cursor
    - conn: database connector
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    transform and process data into the analytics tables on Redshift
    - cur: database cursor
    - conn: database connector
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()
    print ("hello")