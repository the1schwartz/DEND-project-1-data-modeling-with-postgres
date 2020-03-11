import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """Drops existing sparkify database and creates a new one.

    Returns
    -------
    cursor, connection
        cursor and connection for sparkify database
    """

    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student \
            password=student")
    except psycopg2.Error as e:
        print("Error: Could not make connection to studentdb database")
        print(e)
    conn.set_session(autocommit=True)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")
        print(e)

    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    except psycopg2.Error as e:
        print("Error: Could not drop sparkifydb database")
        print(e)

    try:
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE \
            template0")
    except psycopg2.Error as e:
        print("Error: Could not create sparkifydb database")
        print(e)

    # close connection to default database
    conn.close()

    # connect to sparkify database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
            user=student password=student")
    except psycopg2.Error as e:
        print("Error: Could not make connection to sparkifydb database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")
        print(e)

    return cur, conn


def drop_tables(cur, conn):
    """ Drop all existing tables in sparkify database.

    Parameters
    ----------
    cur : cursor
        cursor to the sparkify database connection
    conn : connection
        connection to the sparkify database
    """

    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Could not drop table: " + query)
            print(e)


def create_tables(cur, conn):
    """Creates all tables for sparkify database.

    Parameters
    ----------
    cur : cursor
        cursor to the sparkify database connection
    conn : connection
        connection to the sparkify database
    """

    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Could not create table: " + query)
            print(e)


def main():
    """Drops existing sparkify database and creates new one the tables."""

    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
