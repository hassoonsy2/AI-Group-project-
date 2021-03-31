import psycopg2
from psycopg2 import Error




def connect():
    """This function is the connection with the postgres db"""
    connection = psycopg2.connect(host='localhost', database='huwebshop', user='postgres', password='Xplod_555')
    return connection

c = connect()

def disconnect():
    """This function disconnects the program with the postgres db"""
    return c.close()

def sql_execute(sql,value):
    """This function executes a query on the Postgres db"""
    cur = c.cursor()
    cur.execute(sql,value)


def sql_select(sql):
    """This function select values from the tables on the Postgres db"""
    cur = c.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    return results


def sql_query(sql):
    """This function executes a query on the Postgres db """
    cur = c.cursor()
    cur.execute(sql)
    c.commit()





def select_producten():
    """ This function will Select & count every product from Tabel profiles previously viewed on the Postgres db """

    try:
        return sql_select("""Select Count (distinct category) from products """ )
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


for i