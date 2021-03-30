import psycopg2
from psycopg2 import Error




def connect():
    """This function is the connection with the postgres db"""
    connection = psycopg2.connect(host='localhost', database='huwebshope1', user='postgres', password='Lafa22446688##')
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





def select_profiels_en_producten():
    """ This function will Select & count every product from Tabel profiles previously viewed on the Postgres db """

    try:
        return sql_select("""SELECT profiles_previously_viewed.profid, profiles_previously_viewed.prodid,products.name, products.subcategory,products.targetaudience,COUNT(prodid)

                        FROM profiles_previously_viewed

                        INNER JOIN products ON profiles_previously_viewed.prodid = products.id
                        GROUP BY profiles_previously_viewed.profid, profiles_previously_viewed.prodid,products.name,products.subcategory,products.targetaudience
                         ORDER BY COUNT(prodid) DESC ;""" )
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def select_profiels_en_producten1():
    """ This function will Select & count every product from Tabel profiles previously viewed on the Postgres db """

    try:
        return sql_select("""SELECT orders.sessionsid, sessions.profid,orders.prodid,products.name, products.subcategory,products.targetaudience,COUNT(prodid)

                          FROM orders
                          INNER JOIN products ON orders.prodid = products.id
                          INNER JOIN sessions ON orders.sessionsid = sessions.id

                          GROUP BY orders.sessionsid,sessions.profid, orders.prodid,products.name, products.subcategory,products.targetaudience
                           ORDER BY COUNT(prodid) DESC ;""")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)



Volwassenen = []
Vrouwen = []
Mannen = []
Kinderen = []

t = ["Baby's", "Jongen", "Meisje", "Volwassenen", "Vrouwen", "Kinderen", "Mannen"]

for i in select_profiels_en_producten1():
    print(i)

    if i[5] == t[6] :
        Mannen.append(i)
        continue

    elif i[5] == t[0] or i[5] ==t[1] or i[5] ==t[5] :
        Kinderen.append(i[1:])
        continue

    elif i[5] == t[2] or i[5] ==t[4] :
        Vrouwen.append(i[1:])
        continue



print("orders gefilterd")

for i in select_profiels_en_producten():
    if i[4] == t[6] :
        Mannen.append(i)
        continue

    elif i[4] == t[0] or i[4] ==t[1] or i[4] ==t[5] :
        Kinderen.append(i)
        continue

    elif i[4] == t[2] or i[4] ==t[4] :
        Vrouwen.append(i)
        continue


print("profiles_previously_viewed gefilterd")





sql_query("""DROP TABLE IF EXISTS personas_mannen CASCADE""")


sql_query("""DROP TABLE IF EXISTS personas_vrouwen CASCADE""")


sql_query("""DROP TABLE IF EXISTS personas_kinderen CASCADE""")




sql_query("""CREATE TABLE personas_mannen
                            (prodid VARCHAR  ,
                            name VARCHAR,
                            profid VARCHAR  ,
                            subcategory VARCHAR ,
                            targetaudience VARCHAR,
                            FOREIGN KEY (profid) REFERENCES profiles(id),
                            FOREIGN KEY (prodid) REFERENCES products(id));""")
print("tabel personas_mannen gemaakt ")
sql_query("""CREATE TABLE personas_vrouwen
                            (prodid VARCHAR ,
                            name VARCHAR,
                            profid VARCHAR  ,
                            subcategory VARCHAR ,
                            targetaudience VARCHAR,
                            FOREIGN KEY (profid) REFERENCES profiles(id),
                            FOREIGN KEY (prodid) REFERENCES products(id));""")
print("tabel personas_vrouwen gemaakt ")
sql_query("""CREATE TABLE personas_kinderen
                            (prodid VARCHAR ,
                            name VARCHAR,
                            profid VARCHAR ,
                            subcategory VARCHAR ,
                            targetaudience VARCHAR,
                            FOREIGN KEY (profid) REFERENCES profiles(id),
                            FOREIGN KEY (prodid) REFERENCES products(id));""")
print("tabel personas_kinderen gemaakt ")

c.commit()



try:
    for i in Mannen:
        if len(i[0]) == 24 :
            profid = i[0]
            prodid = i[1]
            name = i[2]
            subcategory = i[3]
            targetaudience = i[4]
            sql_execute(
                "Insert into personas_mannen(profid ,prodid , name, subcategory ,  targetaudience ) VALUES (%s , %s, %s, %s,%s)",
               [profid, prodid, name, subcategory, targetaudience])
    c.commit()
    print("MANNEN Done")

    for i in Vrouwen:
        if len(i[0]) == 24:
            profid = i[0]
            prodid = i[1]
            name = i[2]
            subcategory = i[3]
            targetaudience = i[4]

            sql_execute(
                "Insert into personas_vrouwen(profid ,prodid , name, subcategory ,  targetaudience ) VALUES (%s , %s, %s, %s,%s)",
                [profid, prodid, name, subcategory, targetaudience])

    c.commit()
    print("Vrouwen Done")
    for i in Kinderen:
        if len(i[0]) == 24:
            profid = i[0]
            prodid = i[1]
            name = i[2]
            subcategory = i[3]
            targetaudience = i[4]
            sql_execute(
                "Insert into personas_kinderen(profid ,prodid , name, subcategory ,  targetaudience ) VALUES (%s , %s, %s, %s,%s)",
                [profid, prodid, name, subcategory, targetaudience])
    c.commit()
    print("Kinderenen Done")


except (Exception, psycopg2.DatabaseError) as error:
    print(error)













