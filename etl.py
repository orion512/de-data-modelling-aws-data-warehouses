import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for idx, query in enumerate(copy_table_queries):
        print(f"-- executing query number {idx+1}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for idx, query in enumerate(insert_table_queries):
        print(f"-- executing query number {idx+1}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    with psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())) as conn:
        cur = conn.cursor()
        
        print("Started Loading Staging Tables")
        load_staging_tables(cur, conn)
        print("Started inserting from staging to live")
        insert_tables(cur, conn)


if __name__ == "__main__":
    main()