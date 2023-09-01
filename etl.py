import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries



def load_staging_tables(cur, conn):
    
    """

    Main Module Name - etl.py

    Sub Module Name - load_staging_tables

    Sub Module Name Description - This python module loads the staging songs and staging events tables from the S3 bucket using COPY             statement.

    Input Parameters Details - 

    cur - cursor name
    conn - connection name

    Output Parameters Details - N/A
    
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """

     Main Module Name - etl.py

     Sub Module Name - insert_tables

     Sub Module Name Description - This python module loads the fact and dimension tables within the STAR schema from the staging songs and      staging events tables using INSERT INTO sql commands.

     Input Parameters Details - 

     cur - cursor name
     conn - connection name

     Output Parameters Details - N/A

    """
        
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
    
    print("Calling insert tables module")
    insert_tables(cur, conn)
    print("Closing the cluster connection")
    
    conn.close()

        
if __name__ == "__main__":
    main()