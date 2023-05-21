from flask import jsonify, request
from flask_restful import Resource
from bson.json_util import dumps
from json import loads
from datetime import datetime
from bson.objectid import ObjectId
import pymongo
import gridfs
import base64
from random import randint
import re
from db import graph as db
from datetime import date

class UserAuth(Resource):
    
    def post(self):
        try:
            data = request.get_json()
            username = data['username']
            password = data['password']
            query = "MATCH (n:User {username: '%s'}) return n"%(username)

            result = db.run(query)
            equal_password = False
            for record in result:
                user_node = record["n"]
                equal_password = user_node['Password'] == password
            
            return jsonify({"code": "200", "status": "authenticated"}) if equal_password else jsonify({"code": "401", "status": "error"})
        except:
            return jsonify({"code": "400", "status": "error"})
        
class WatchedMovie(Resource):
    def get(self,):
        try:
            data = request.headers
            username = data['username']
            done = data['done']
            
            query = "MATCH (u:User {username: '%s'})-[r:WATCHED]->(m:Movie) WHERE r.Finished = %s return m"%(username,done)
            print(query)
            result = db.run(query)  
            movies = []
            for record in result:
                res = record['m']
                movies.append({"Title": res['Title'], "image": res['link_img'], "link": res['Link_trailer']})
            return jsonify(movies)
        except:
            return jsonify({"code": "400", "status": "error"})
        
    def put(self): 
        try:
            data = request.get_json()
            username = data['username']
            movie = data['title']

            query = "RETURN EXISTS((:User {username: '%s'})-[:WATCHED]->(:Movie {Title: '%s'}))" % (username, movie)
            result = db.run(query).data()
            relationship_exists = False
            for keys in result[0]:
                relationship_exists = result[0][keys]

            if relationship_exists:
                properties = "r.Last_seen= timestamp(),"
                if 'finished' in data:
                    properties += "r.Finished= %s,"%data['finished']
                if 'liked' in data:
                    properties += "r.Liked= %s,"%data['liked'] 
                if 'rating' in data:
                    properties += "r.Rating= %s,"%data['rating']
                
                properties = properties[:-1]

                query = "MATCH (u:User {username: '%s'})-[r:WATCHED]->(m:Movie {Title: '%s'}) SET %s"%(username, movie, properties)
                print(query)
            else:
                query = "MATCH (u:User {username: '%s'}), (m:Movie {Title: '%s'}) MERGE (u) -[r:WATCHED {Finished: false, Liked: false, Last_seen: timestamp()}]-> (m)"%(username, movie)
            
            db.run(query)
            
            return jsonify({"code": "200", "status": "updated"})
        except:
            return jsonify({"code": "400", "status": "error"})


class User(Resource):

    def post(self):
        try:
            data = request.get_json()
            username = data['username']
            password = data['password']
            age = data['age']
            subscription = data['subscription']

            query = "MATCH (n:User {username: '%s'}) return n"%(username)
            result = db.run(query).data()
            if result:
                return jsonify({"code": "401", "status": "User already exists"})
            
            current_date = date.today()
            
            query = "MERGE (u:User {username: '%s', Password: '%s', Age: %s, Registration_date: date('%s'), Subscription_type: '%s'})"%(username, password, age, str(current_date), subscription)
            result = db.run(query)

            if result.consume().counters.nodes_created > 0:
                return jsonify({"code": "200", "status": "registered"})
            else:
                return jsonify({"code": "400", "status": "error"})
        except:
            return jsonify({"code": "400", "status": "error"})

class RandomMovie(Resource):

    def get(self):
        try:
            query = "MATCH (m:Movie) WITH m, rand() AS random ORDER BY random LIMIT 1 RETURN m"
            result = db.run(query)
            result = result.data()[0]['m']
            response = {"Title": result['Title'], "image": result['link_img'], "link": result['Link_trailer']}
            return jsonify(response)
        except:
            return jsonify({"code": "400", "status": "error"})


class SuggestedMovie(Resource):

    def get(self):
        try: 
            query = "MATCH (m:Movie) return m"
            result = db.run(query)
            movies = []
            for record in result:
                res = record['m']
                movies.append({"Title": res['Title'], "image": res['link_img'], "link": res['Link_trailer']})
            return jsonify(movies)
        except:
            return jsonify({"code": "400", "status": "error"})

class Movie(Resource):

    def get(self):
        try:
            data = request.headers
            username = data['username']
            movie = data['movie']

            query = "MATCH (u:User {username: '%s'})-[r:WATCHED]->(m:Movie {Title: '%s'}) return m, r"%(username, movie)
            
            result = db.run(query)
            record = result.single()
            relation = record['r']
            properties = relation._properties
            response = {'liked': properties['Liked']}

            return jsonify(response)
        except:
            return jsonify({"code": "400", "status": "error"})
