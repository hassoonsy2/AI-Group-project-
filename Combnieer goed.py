
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


def sql_execute(sql, value):
    """This function executes a query on the Postgres db"""
    cur = c.cursor()
    cur.execute(sql, value)
    results = cur.fetchall()
    return results


def sql_execute2(sql, value):
    """This function executes a query on the Postgres db"""
    cur = c.cursor()
    cur.execute(sql, value)


def sql_select(sql):
    """This function select values from the tables on the Postgres db"""
    cur = c.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    c.commit()
    return results


def sql_query(sql):
    """This function executes a query on the Postgres db """
    cur = c.cursor()
    cur.execute(sql)



sql_query("DROP TABLE IF EXISTS combinatie_recommendations CASCADE")



sql_query("""CREATE TABLE combinatie_recommendations
            ( prodid varchar  ,
                combinatie_nr integer ,
                FOREIGN KEY (prodid) REFERENCES products(id));""")
c.commit()


def picking_freq_orders():

    return sql_select("""SELECT orders.sessionsid,
                       COUNT(prodid)
                       FROM orders

                       GROUP BY orders.sessionsid 
                       ORDER BY COUNT(prodid) asc ;""")



def filtring(result):
    id_list = []

    for i in result :
        if i[1] >= 4 :
            id_list.append(i)

        else:
            continue


    return id_list




def picking_products(profiels_list_orders):
    ids = []

    combinatie = 0
    for i in profiels_list_orders :
        ids_orders =sql_execute("""select orders.prodid  
                        from orders
                        where orders.sessionsid = (%s)
                        ;""", [i[0]])

        print(ids_orders)
        ids.append(ids_orders)
        for j in ids_orders:
            print(j[0])
            sql_execute2("insert into combinatie_recommendations (prodid ,combinatie_nr ) values (%s , %s)",
                         [j[0], combinatie])
        c.commit()

        combinatie += 1







    return ids



x= filtring(picking_freq_orders())
picking_products(x)

