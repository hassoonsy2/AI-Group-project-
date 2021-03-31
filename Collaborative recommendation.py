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





def select_profiels_en_producten_from_previously_viewed():
    """ This function will Select profiels id's and products id's from tabel previously_viewed """

    try:
        return sql_select("""SELECT profiles_previously_viewed.profid, profiles_previously_viewed.prodid,products.name, products.subcategory,products.targetaudience,COUNT(prodid)

                        FROM profiles_previously_viewed

                        INNER JOIN products ON profiles_previously_viewed.prodid = products.id
                        GROUP BY profiles_previously_viewed.profid, profiles_previously_viewed.prodid,products.name,products.subcategory,products.targetaudience
                         ORDER BY COUNT(prodid) DESC ;""" )
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def select_profiels_en_producten_from_orders():
    """ This function will Select profiels id's and products id's from tabel Orders   """

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

gender = ["Baby's", "Jongen", "Meisje", "Volwassenen", "Vrouwen", "Kinderen", "Mannen"]

for i in select_profiels_en_producten_from_orders():
    if i[5] == gender[6] :
        Mannen.append(i)
        continue

    elif i[5] == gender[0] or i[5] == gender[1] or i[5] == gender[5] :
        Kinderen.append(i[1:])
        continue

    elif i[5] == gender[2] or i[5] == gender[4] :
        Vrouwen.append(i[1:])
        continue



print("Kennis van orders zijn gefilterd")

for i in select_profiels_en_producten_from_previously_viewed():
    if i[4] == gender[6] :
        Mannen.append(i)
        continue

    elif i[4] == gender[0] or i[4] == gender[1] or i[4] == gender[5] :
        Kinderen.append(i)
        continue

    elif i[4] == gender[2] or i[4] == gender[4] :
        Vrouwen.append(i)
        continue


print("Kennis van profiles_previously_viewed zijn gefilterd")





sql_query("""DROP TABLE IF EXISTS personas_mannen CASCADE""")


sql_query("""DROP TABLE IF EXISTS personas_vrouwen CASCADE""")


sql_query("""DROP TABLE IF EXISTS personas_kinderen CASCADE""")


sql_query("""DROP TABLE IF EXISTS personas_recommendations CASCADE""")




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
sql_query("""CREATE TABLE personas_recommendations
                            (prodid VARCHAR ,
                            targetaudience VARCHAR,
                            FOREIGN KEY (prodid) REFERENCES products(id));""")

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

def personas_recommendations():
    """ This function will select few products from each peronas tabels in a personas_recommendations tabel  """
    id_list= []
    resul_mannen = sql_select("""SELECT prodid , targetaudience
                            FROM personas_mannen
                             LIMIT 6 ;""")
    c.commit()

    result_vrouwen =sql_select("""SELECT prodid ,  targetaudience
                            FROM personas_vrouwen
                             LIMIT 6 ;""")
    c.commit()

    result_kinderen =sql_select("""SELECT prodid ,  targetaudience
                            FROM personas_kinderen
                            where targetaudience = 'Kinderen'
                             LIMIT 6 ;""")
    c.commit()
    for elment in resul_mannen :
        id_list.append(elment)
        continue
    for elment in result_vrouwen:
        id_list.append(elment)
        continue
    for elment in result_kinderen:
        id_list.append(elment)
        continue
    print(id_list)
    for i in id_list:
        prodid = i[0]
        targetaudience = i[1]
        sql_execute("Insert into personas_recommendations( prodid, targetaudience ) VALUES (%s , %s)",
                    [prodid,  targetaudience])
    c.commit()
    return id_list




personas_recommendations()







