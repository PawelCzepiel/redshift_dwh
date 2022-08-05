import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
  Description: Function loads the staging logs&songs data tables 
                (as per copy_table_queries in sql_queries.py) 
               from json files located in s3 bucket defined in config file. 
  Arguments:
               cur: the cursor object. 
               conn: connection to Redshift aws cluster.
  Returns:
               None
  """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
  Description: Function loads the star schema tables in DWH 
                (as per insert_table_queries in sql_queries.py) 
               from previously populated staging_events and staging_songs.
  Arguments:
               cur: the cursor object. 
               conn: connection to Redshift aws cluster.
  Returns:
               None
  """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
  Description: Function reads AWS parameters from dwg.cfg,
               establishes the connection wtih Redshift cluster
               and executes s3=>staging=>star_schema data transfer
  Arguments:
               None
  Returns:
               None
  """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()