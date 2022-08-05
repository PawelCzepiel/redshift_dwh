import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
  Description: Function deletes all tables (staging and star schema)
               from Redshift cluster.
  Arguments:
               cur: the cursor object. 
               conn: connection to Redshift aws cluster.
  Returns:
               None
  """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
  Description: Function creates all tables (staging and star schema)
               on Redshift cluster.
  Arguments:
               cur: the cursor object. 
               conn: connection to Redshift aws cluster.
  Returns:
               None
  """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
  Description: Function reads AWS parameters from dwg.cfg,
               establishes the connection wtih Redshift cluster
               and resets all tables (staging and star schema).
  Arguments:
               None
  Returns:
               None
  """
    config = configparser.ConfigParser()
    config.read('../dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()