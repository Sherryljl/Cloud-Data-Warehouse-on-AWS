import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    copy data from datasource to staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    insert data from staging tables to tables in dimensional model
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Get cluster configuration info to connect to an existing database
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #connect to Redshift database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #loading data into staging tables
    load_staging_tables(cur, conn)
    #insert data into analytical tables
    insert_tables(cur, conn)
    
    #close database connection
    conn.close()


if __name__ == "__main__":
    main()