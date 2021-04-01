from flask import Flask, request, session, render_template, redirect, url_for, g
from flask_restful import Api, Resource, reqparse
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import psycopg2

app = Flask(__name__)
api = Api(app)

# We define these variables to (optionally) connect to an external MongoDB
# instance.
envvals = ["MONGODBUSER","MONGODBPASSWORD","MONGODBSERVER"]
dbstring = 'mongodb+srv://{0}:{1}@{2}/test?retryWrites=true&w=majority'

# Since we are asked to pass a class rather than an instance of the class to the
# add_resource method, we open the connection to the database outside of the 
# Recom class.
load_dotenv()
if os.getenv(envvals[0]) is not None:
    envvals = list(map(lambda x: str(os.getenv(x)), envvals))
    client = MongoClient(dbstring.format(*envvals))
else:
    client = MongoClient()
database = client.huwebshop

def connect():
    """This function is the connection with the postgres db"""
    connection =  psycopg2.connect(host='localhost', database='huwebshope1', user='postgres', password='Lafa22446688##')
    return connection

c = connect()
def sql_select(sql):
    """This function select values from the tables on the Postgres db"""

    cur = c.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    return results

def sql_execute(sql,value):
    """This function executes a query on the Postgres db"""
    cur = c.cursor()
    cur.execute(sql,value)
    results = cur.fetchall()
    return results


class Recom(Resource):
    """ This class represents the REST API that provides the recommendations for
    the webshop. At the moment, the API simply returns a random set of products
    to recommend."""

    mannen_id = ['5af09037172c9d0001c12831', '5c39c62017aacf000110d45a', '5a3c1f9da82561000175591d', '5acf7b7b429ba4000195c693',
                 '5c31e93715cedd000121f907', '5c1ff4e0d4bc850001b68415', '5a9ac25ec443f10001ecc95c','5b64c31c40388000014a0f5a',
                 '5b36d14a2e3d600001bd2a44', '5bf00b6422ac730001cdfa24', '5b38ba412e3d60000196d581', '5bc8641cfd58b60001000316',
                 '5bf7d9004dad87000162829d', '5bddd8871e50b2000163ae36', '5b1fae035af3010001051e9a', '5b4dbeb578d4f90001ba08a3',
                 '5a0997a1a56ac6edb4efaab3', '5a7c974a4e0e980001caf309', '5b8c1bf997d556000170f262', '5bb6437a7c4e350001b90690',
                 '5bb2557ea6578c0001c10625', '5b5393574a76420001238237', '5bcc550791c0f500011eea5c', '5b59e00bf88d0300018f2133']
    vrouwen_id = ['5a9ef773a92b240001a673f2', '59dcea98a56ac6edb4d7a782', '5b783e964fd4640001763f5a', '5b03d19f1eabe40001699ede',
                  '5b7c03914fd46400013302bb', '5b7c03914fd46400013302bb', '5b2e439741b2d00001db18dd', '59dceabaa56ac6edb4d7ca7f',
                  '5a3a37a2a825610001bc36a8', '5b913563724039000149dbe3', '5b83dfd183e57a0001d08dac', '59dcef9ba56ac6edb4df30f7',
                  '59dcec16a56ac6edb4d93f68', '5be9711696e43d00016b4f28', '59dcf05aa56ac6edb4dfe363','59dcec16a56ac6edb4d93f68']
    kinderen_id = ['5bdf68482a3017000185ef8f', '59dceb38a56ac6edb4d85167', '5bc9f26efd58b6000143d8c8', '5a0fa5e9a56ac6edb4c7c477',
                   '5bc9f26efd58b6000143d8c8', '5b55eca44a76420001d3e0be', '5b748ee397d5560001983e1b', '5bcb79a291c0f5000101253c',
                   '5bb26141a6578c0001c116a0', '5a09979ea56ac6edb4ef7b99', '5ae4995582f803000187025d', '5b55ac1ce3840d0001cda710',
                   '59dce7c0a56ac6edb4ca7888', '5a140809a56ac6edb4fb3242', '59dcf04ca56ac6edb4dfd3ca', '5b444ca161afda0001c56cb6',]
    test_id = ['59dce304a56ac6edb4c118e4']

    def get(self,profileid,productid,  count ):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """
        randcursor = database.products.aggregate([{ '$sample': { 'size': count } }])


        if profileid in self.test_id :
            if productid != 0 :
                prdids = self.soortgelijk(productid)

                return prdids, 200


            prdids = self.simpel_reco()
            return prdids, 200



        elif profileid in self.mannen_id:
            prdids = self.Mannen()
            return prdids, 200

        elif profileid in self.vrouwen_id:
            prdids = self.Vrouwen()
            return prdids, 200

        elif profileid in self.kinderen_id:
            prdids = self.kinderen()
            return prdids, 200

        else:
            prdids = self.simpel_reco()
            return prdids, 200







    def soortgelijk(self, productid):
        ids = []
        for i in sql_execute(""" Select subsubcategory from products 
                                          Where id = (%s);""", [productid]) :
            subsubcatgo = i
            c.commit()
            for i in sql_execute("""Select prodid from soortgelijke_producten 
                        where  subsubcategory = (%s)
                        LIMIT 4;""",[subsubcatgo]) :
                ids.append(i[0])
        return ids




    def simpel_reco(self):
        id_lists = []
        result = sql_select(""" SELECT prodid , name
            FROM BEST_seller
            WHERE Counter > 1000
            LIMIT 4;""")
        c.commit()
        for elment in result :
            id_lists.append(elment[0])
        return id_lists


    def Mannen(self):
        id_list= []
        result = sql_select("""SELECT prodid
                                FROM personas_recommendations WHERE targetaudience = 'Mannen'
                                 LIMIT 4 ;""")
        c.commit()
        for elment in result:
            id_list.append(elment[0])
        return id_list


    def Vrouwen(self):
        id_list= []
        result = sql_select("""SELECT prodid
                                FROM personas_recommendations WHERE targetaudience = 'Vrouwen' 
                                 LIMIT 5  ;""")
        c.commit()
        for elment in result:
            id_list.append(elment[0])
        return id_list


    def kinderen(self):
        id_list= []
        result = sql_select("""SELECT prodid
                                FROM personas_recommendations WHERE targetaudience <> 'Vrouwen'  and targetaudience <> 'Mannen'
                                 LIMIT 4 ;""")
        c.commit()
        for elment in result:
            id_list.append(elment[0])
        return id_list




# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<string:productid>/<int:count>")
