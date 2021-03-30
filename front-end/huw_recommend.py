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
    global connection
    connection = psycopg2.connect(host='localhost', database='huwebshop', user='postgres', password='Xplod_555')
    return connection

def sql_select(sql):
    """This function select values from the tables on the Postgres db"""
    c = connect()
    cur = c.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    return results

def commit():
   """This function will cpmmit  a query on the Postgres db """
   c = connect()
   c.commit()

class Recom(Resource):
    """ This class represents the REST API that provides the recommendations for
    the webshop. At the moment, the API simply returns a random set of products
    to recommend."""

    def get(self,profileid, productdetail,  count ):
        """ This function represents the handler for GET requests coming in
        through the API. It currently returns a random sample of products. """
        randcursor = database.products.aggregate([{ '$sample': { 'size': count } }])
        if productdetail == "producten" :
            prdids = self.simpel_reco()

        else:
            prdids = self.Mannen()
        return prdids, 200

    def simpel_reco(self):
        id_lists = []
        result = sql_select(""" SELECT prodid , name
            FROM BEST_seller
            WHERE Counter > 1000
            LIMIT 8;""")
        commit()
        for elment in result :
            id_lists.append(elment[0])
        return id_lists

    def Mannen(self):
        id_list= []
        result = sql_select("""SELECT prodid , name 
                                FROM personas_mannen
                                 LIMIT 4 ;""")
        commit()
        for elment in result :
            id_list.append(elment[0])
        return id_list




# This method binds the Recom class to the REST API, to parse specifically
# requests in the format described below.
api.add_resource(Recom, "/<string:profileid>/<string:productdetail>/<int:count>")
