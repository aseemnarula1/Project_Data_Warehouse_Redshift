import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries



def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Entering the ETL module")
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Calling load staging table cluster connection")
    load_staging_tables(cur, conn)
    
    print("Calling inser tables module")
    insert_tables(cur, conn)
    print("Closing the cluster connection")
    
    conn.close()

        
if __name__ == "__main__":
    main()