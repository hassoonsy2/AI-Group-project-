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
    results = cur.fetchall()
    return results

def sql_execute2(sql,value):
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



def select_most_sold_products_from_personas_mannen():
    """ This function will Select & count every product from Tabel Orders on the Postgres db """
    return sql_select("""SELECT prodid, name,
                       COUNT(*)
                       FROM personas_mannen
                      
                       GROUP BY prodid ,name 
                       ORDER BY COUNT(*) DESC ; """)


def select_most_sold_products_from_personas_vrouwen():
    """ This function will Select & count every product from Tabel Orders on the Postgres db """
    return sql_select("""SELECT prodid, name,
                       COUNT(*)
                       FROM personas_vrouwen

                       GROUP BY prodid ,name 
                       ORDER BY COUNT(*) DESC ; """)


def select_most_sold_products_from_personas_kinderen():
    """ This function will Select & count every product from Tabel Orders on the Postgres db """
    return sql_select("""SELECT prodid, name,
                       COUNT(*)
                       FROM personas_kinderen

                       GROUP BY prodid ,name 
                       ORDER BY COUNT(*) DESC ; """)


def insert_most_sold_products_from_personas_recommendations():
    ids=[]
    target = ["Mannen_Popular" , "Vrouwen_Popular", "Kinderen_Popular"]

    result_mannen = select_most_sold_products_from_personas_mannen()

    result_vrowuen = select_most_sold_products_from_personas_vrouwen()

    result_kinderen = select_most_sold_products_from_personas_kinderen()

    for i in result_mannen[:6]:
        prodoct = i[0],target[0]
        ids.append(prodoct)
    for i in result_vrowuen[:6]:
        prodoct = i[0], target[1]
        ids.append(prodoct)
    for i in result_kinderen[:6]:
        prodoct = i[0], target[2]
        ids.append(prodoct)

    for i in ids :
        proid = i[0]
        targetaudience = i[1]
        sql_execute2("Insert into personas_recommendations( prodid, targetaudience ) VALUES (%s , %s)",
                    [proid, targetaudience])
    c.commit()
    print("doen")





insert_most_sold_products_from_personas_recommendations()