import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):

    '''

    Main Module Name - create_tables.py

    Sub Module Name - drop_tables

    Sub Module Name Description - This python module drops the tables in the Amazon Redshift database for the open cursor passed as an input     parameter, executes the query for DROP statement and commits the action.

    Input Parameters Details - 

    cur - cursor name
    conn - connection name

    Output Parameters Details - N/A


    '''

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):

    '''

    Main Module Name - create_tables.py

    Sub Module Name - create_tables

    Sub Module Name Description - This python module creates the tables in the Amazon Redshift database for the open cursor passed as an         input parameter, executes the query for CREATE statement and commits the action.

    Input Parameters Details - 

    cur - cursor name
    conn - connection name

    Output Parameters Details - N/A

    '''
   
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    '''

    Main Module Name - create_tables.py

    Sub Module Name - main

    Sub Module Name Description - 

    This python main module first calls the config parser module that reads the values from the config file, connect to the Amazon               Redshift cluster, creates the tables in the Amazon Redshift database for the open cursor passed as an input parameter, executes the         query for DROP/CREATE statement and commits the action and finally close the connection.

    Input Parameters Details - N/A

    Output Parameters Details - N/A

    '''

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()