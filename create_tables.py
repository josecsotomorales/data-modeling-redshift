import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ drop database tables from drop_table_queries, 
    a list with DROP statements
    """
    for query in drop_table_queries:
        print(f"Executing drop: {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ create database tables from create_table_queries, 
    a list with INSERT statements
    """
    for query in create_table_queries:
        print(f"Executing create: {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to redshift')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to redshift')
    cur = conn.cursor()

    print('Dropping existing tables')
    drop_tables(cur, conn)
    
    print('Creating tables')
    create_tables(cur, conn)

    conn.close()
    print('Create tables ok')


if __name__ == "__main__":
    main()