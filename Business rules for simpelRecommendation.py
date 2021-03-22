import psycopg2
from psycopg2 import Error






def connect():
    """This function is the connection with the postgres db"""

    connection = psycopg2.connect(host='localhost', database='huwebshop', user='postgres', password='Xplod_555')
    return connection

def disconnect():
    """This function disconnects the program with the postgres db"""
    con = connect()
    return con.close()

def sql_execute(sql,value):
    """This function executes a query on the Postgres db"""
    c = connect()
    cur = c.cursor()
    cur.execute(sql,value)


def sql_select(sql):
    """This function select values from the tables on the Postgres db"""
    c = connect()
    cur = c.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    return results


def sql_query(sql):
    """This function executes a query on the Postgres db """
    c = connect()
    cur = c.cursor()
    cur.execute(sql)

def commit():
   """This function will cpmmit  a query on the Postgres db """
   c = connect()
   c.commit()




#                                                                                                     >> { Content-Based Filtering } <<<


def select_most_sold_products():
    """ This function will Select & count every product from Tabel Orders on the Postgres db """

    try:
        return sql_select("""SELECT orders.prodid, products.name,
                       COUNT(*)
                       FROM orders
                       INNER JOIN products ON Orders.prodid = products.id
                       GROUP BY prodid ,products.name 
                       ORDER BY COUNT(*) DESC ; """)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)



            

def best_seller():
    """This function will Create Tabel Best seller on the Postgres db  """
    try:
         sql_query("DROP TABLE IF EXISTS Best_seller CASCADE")

         sql_query("""CREATE TABLE Best_seller                                
                            (prodid VARCHAR PRIMARY KEY,                        
                            name VARCHAR,                                       
                            Counter INTEGER ,                                   
                            FOREIGN KEY (prodid) REFERENCES products(id));""")


         results = select_most_sold_products()

        #Right , now we can insert the result into the Tabel

         for row  in results:
             prodid = row[0]
             name = row[1]
             cont = row[2]
             sql_execute("Insert into Best_seller(prodid ,name , Counter ) VALUES (%s , %s, %s)",[prodid,name,cont])
         commit()


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    print("Content Filtering {Best Seller } is Done ")


best_seller()

def select_most_viewed_products():
    """ This function will Select & count every product from Tabel profiles previously viewed on the Postgres db """

    try:
        return sql_select("""SELECT profiles_previously_viewed.prodid, products.name,
                        COUNT(*)
                        FROM profiles_previously_viewed
                        INNER JOIN products ON profiles_previously_viewed.prodid = products.id
                        GROUP BY prodid ,products.name 
                        ORDER BY COUNT(*) DESC ; """)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)



def most_viwed_products():
    """This function will Create Tabel Most viwed products on the Postgres db  """
    try:
        sql_query("DROP TABLE IF EXISTS most_viwed_products CASCADE")

        sql_query("""CREATE TABLE most_viwed_products                          
                        (prodid VARCHAR PRIMARY KEY,                  
                          name VARCHAR,                                 
                          Counter INTEGER ,                             
                          FOREIGN KEY (prodid) REFERENCES products(id));""")

        commit()

        results = select_most_viewed_products()

        #Right , now we can insert the result into the Tabel

        for row in results:
            prodid = row[0]
            name = row[1]
            cont = row[2]
            sql_execute("Insert into most_viwed_products(prodid ,name , Counter ) VALUES (%s , %s, %s)",[prodid,name,cont])

        commit()

    except(Exception, psycopg2.DatabaseError) as error:

        print(error)

    print("Content Filtering {Most viwed products } is Done ")



most_viwed_products()






disconnect()