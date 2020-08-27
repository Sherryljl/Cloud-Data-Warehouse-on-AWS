import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all tables if they exist in database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging tables and analytical tables in database
    """ 
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Get cluster configuration info to connect to an existing database
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #connect to Redshift database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Drop tables in case they've already existed in connected database
    drop_tables(cur, conn)
    print("All existing tables are successfully dropped.")
    print("--------------------------------------------------------")
    # Create tables in Redshift
    create_tables(cur, conn)
    print("All tables are successfully created.")

    conn.close()
    print("Connection to database is closed.")


if __name__ == "__main__":
    main()