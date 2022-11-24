import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for idx, query in enumerate(create_table_queries):
        print(f"-- executing query number {idx+1}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    with psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())) as conn:
        cur = conn.cursor()

        print("Started dropping tables")
        drop_tables(cur, conn)
        print("Started creating tables")
        create_tables(cur, conn)


if __name__ == "__main__":
    main()