import psycopg2
from psycopg2 import Error




def connect():
    """This function is the connection with the postgres db"""
    connection =  psycopg2.connect(host='localhost', database='huwebshope1', user='postgres', password='Lafa22446688##')
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


sql_query("""DROP TABLE IF EXISTS soortgelijke_producten CASCADE""")

sql_query("""CREATE TABLE soortgelijke_producten
                                (prodid VARCHAR PRIMARY key,

                                subsubcategory VARCHAR ,

                                FOREIGN KEY (prodid) REFERENCES products(id));""")
print("Tabel soortgelijke_producten gemaakt !")

def soort_gelijke():
    """ This function will select all the diffrents types of subsubcategory and will insert 4 product for evrey subsubcategory """
    subsubcategory_list = []
    try:
        result = sql_select("""Select distinct subsubcategory from products 
                    Where subsubcategory NOTNULL """)

        for i in result :
            subsubcategory_list.append(i)

        c.commit()

        for i in subsubcategory_list :
            subsubcategory = i
            producten_ids =  sql_execute("""Select id , subsubcategory  from products 
                                          Where subcategory NOTNULL 
                                          and subsubcategory NOTNULL 
                                          and subsubcategory = (%s)  
                                          limit 4
                                           ;""", [subsubcategory])
            c.commit()
            for i in producten_ids :
                prodid = i [0]
                subsubcategory = i[1]

                sql_execute2("insert into  soortgelijke_producten (prodid ,subsubcategory) values (%s ,%s )",[prodid, subsubcategory])
                c.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


soort_gelijke()