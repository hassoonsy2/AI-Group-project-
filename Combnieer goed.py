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
sql_query("DROP TABLE IF EXISTS combinatie_recommendations CASCADE")
sql_query("""CREATE TABLE combinatie_recommendations 
            ( prodid varchar  , 
                combinatie_nr integer ,
                FOREIGN KEY (prodid) REFERENCES products(id));""")
c.commit()
def picking_freq():

    t = []
    for i in sql_select("""select  sessionsid ,count(distinct prodid) as aantal from orders
                group by sessionsid   
                        
                        ; """) :


        if i[1] >= 4 :
            t.append(i[0])


        else:
            continue




    c.commit()
    return t
counter = 0
print(len(picking_freq()))

for i in picking_freq() :
    s= sql_execute("""select orders.prodid  
         from orders
      
         where orders.sessionsid = (%s);""", [i])
    for j in s:
        print(j[0])
        sql_execute2("insert into combinatie_recommendations (prodid ,combinatie_nr ) values (%s , %s)",[j , counter])
    c.commit()

    counter+=1


c.commit()



