import psycopg2
from config.config import config_db
import os

CONFIG_PATH = os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                        "config"), "database.ini")


def connect_db():
    """
    :return: returns, psycopg2's connection(object that communicates with DB)
    """
    try:
        # read connection parameters
        params: dict = config_db(filename=CONFIG_PATH, section='postgresql')
        # connect to the PostgreSQL server
        connection = psycopg2.connect(**params) # kwargs argument, instead of writing host=params['localhost'],..
        # TODO add user, host and port in print
        print(f"Connected to the PostgreSQL database {params['database']}")
        return connection

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def table_exits(cursor, table_name):
    cursor.execute("SELECT EXISTS (SELECT * from information_schema.tables where table_name=%s)", (table_name,))
    return cursor.fetchone()[0]


if __name__ == '__main__':
    conn = connect_db()
    cursor = conn.cursor()
    res = table_exits(cursor, table_name='company_names')
    print(res)
