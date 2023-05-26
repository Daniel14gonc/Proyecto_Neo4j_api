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
            query = "MATCH (n:USER {Username: '%s'}) return n"%(username)

            result = db.run(query)
            equal_password = False
            for record in result:
                user_node = record["n"]
                equal_password = user_node['Password'] == password
            
            return jsonify({"code": "200", "status": "authenticated"}) if equal_password else jsonify({"code": "401", "status": "error"})
        except:
            return jsonify({"code": "400", "status": "error"})
        
    
class MyAccount(Resource):

    def get(self):
        try:
            data = request.headers
            username = data['username']
            query = "MATCH (n:USER {Username: '%s'}) return n"%(username)
            result = db.run(query).single()
            subscription = result['n']._properties['Subscription_type']

            return jsonify({"subscription": subscription})
        except:
            return jsonify({"code": "400", "status": "error"})
        
    def put(self):
        try:
            data = request.get_json()
            username = data['username']
            sub = data['suscription']
            query ="MATCH (u:USER) WHERE u.Username='%s' set u.Subscription_type='%s'"%(username,sub)
            db.run(query)
            
            return jsonify({"code": "200", "status": "updated"})
        except:
            return jsonify({"code": "400", "status": "error"})

        
        
class WatchedMovie(Resource):
    
    def get(self,):
        try:
            data = request.headers
            username = data['username']
            done = data['done']
            
            query = "MATCH (u:USER {Username: '%s'})-[r:WATCHED]->(m:MOVIE) WHERE r.Finished = %s return m"%(username,done)
            result = db.run(query)  
            movies = []
            for record in result:
                res = record['m']
                movies.append({"Title": res['Title'], "image": res['Link_img'], "link": res['Link_trailer']})
            return jsonify(movies)
        except:
            return jsonify({"code": "400", "status": "error"})
        
    def put(self): 
        try:
            data = request.get_json()
            username = data['username']
            movie = data['title']

            query = "RETURN EXISTS((:USER {Username: '%s'})-[:WATCHED]->(:MOVIE {Title: '%s'}))" % (username, movie)
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
                    if data['rating'] != '':
                        properties += "r.Rating= %s,"% float(data['rating'])
                properties = properties[:-1]

                query = "MATCH (u:USER {Username: '%s'})-[r:WATCHED]->(m:MOVIE {Title: '%s'}) SET %s"%(username, movie, properties)
            else:
                query = "MATCH (u:USER {Username: '%s'}), (m:MOVIE {Title: '%s'}) MERGE (u) -[r:WATCHED {Finished: false, Liked: false, Last_seen: timestamp()}]-> (m)"%(username, movie)

            db.run(query)
            
            return jsonify({"code": "200", "status": "updated"})
        except:
            return jsonify({"code": "400", "status": "error"})
class Admin(Resource):
    
    def get(self):
        try:
            data = request.headers
            query = data['query']
            print(query)
            result = db.run(query)
            response = str(result.consume().counters)
            response = eval(response)
            if len(response) == 0:
                response = {"message": "no changes made"}
            print(result)
            print(result.consume().counters)

            response = dict(response)

            return jsonify({"message": str(response)[1:-1]})
        except Exception as e:
            try:
                return jsonify({"message": e.message})
            except:
                return jsonify({"message": "error"})

class User(Resource):

    def post(self):
        try:
            data = request.get_json()
            username = data['username']
            password = data['password']
            age = data['age']
            subscription = data['subscription']

            query = "MATCH (n:USER {Username: '%s'}) return n"%(username)
            result = db.run(query).data()
            if result:
                return jsonify({"code": "401", "status": "USER already exists"})
            
            current_date = date.today()
            
            query = "MERGE (u:USER {Username: '%s', Password: '%s', Age: %s, Registration_date: date('%s'), Subscription_type: '%s'})"%(username, password, age, str(current_date), subscription)
            result = db.run(query)

            if result.consume().counters.nodes_created > 0:
                return jsonify({"code": "200", "status": "registered"})
            else:
                return jsonify({"code": "400", "status": "error"})
        except:
            return jsonify({"code": "400", "status": "error"})
        
    def delete(self):
        try:
            username = request.headers['user']
            query = "MATCH (u:USER {Username: '%s'}) DETACH DELETE u"%username
            db.run(query)
            return jsonify({"code": "200", "status": "deleted"})
        except:
            return jsonify({"code": "400", "status": "error"})

class RandomMovie(Resource):

    def get(self):
        try:
            query = "MATCH (m:MOVIE) WITH m, rand() AS random ORDER BY random LIMIT 1 RETURN m"
            result = db.run(query)
            result = result.data()[0]['m']
            response = {"Title": result['Title'], "image": result['Link_img'], "link": result['Link_trailer']}
            return jsonify(response)
        except:
            return jsonify({"code": "400", "status": "error"})

# TODO: Esta es la clase donde debemos hacer la recomendacion
class SuggestedMovie(Resource):

    def get(self):
        try: 
            query = "MATCH (m:MOVIE) return m"
            result = db.run(query)
            movies = []
            for record in result:
                res = record['m']
                movies.append({"Title": res['Title'], "image": res['Link_img'], "link": res['Link_trailer']})
            return jsonify(movies)
        except:
            return jsonify({"code": "400", "status": "error"})

class Movie(Resource):

    def get(self):
        # try:
        data = request.headers
        username = data['username']
        movie = data['movie']

        print(username, movie)

        query = "MATCH (u:USER {Username: '%s'})-[r:WATCHED]->(m:MOVIE {Title: '%s'}) return m, r"%(username, movie)
        
        result = db.run(query)
        record = result.single()
        if 'r' in record:
            relation = record['r']
            properties = relation._properties
            if "Rating" not in properties:
                properties['Rating'] = ""
            response = {'liked': properties['Liked'],'rating': properties['Rating'],'started':True}
        else:
            response = {'liked': False,'rating': "",'started':False}
        
        return jsonify(response)
        # except:
        #     return jsonify({"code": "400", "status": "error"})

class AllMovies(Resource):
    def get(self):
        try: 
            query = "MATCH (m:MOVIE) return m"
            result = db.run(query)
            movies = []
            for record in result:
                res = record['m']
                movies.append({"Title": res['Title'], "image": res['Link_img'], "link": res['Link_trailer']})
            return jsonify(movies)
        except:
            return jsonify({"code": "400", "status": "error"})
        
class Saga(Resource):

    def get(self):
        try:
            query = "MATCH (s:SAGA) return s"
            result = db.run(query)
            sagas = []
            for record in result:
                res = record['s']
                sagas.append({"name": res['Name'], "image": res['saga_image']})
            return jsonify(sagas)
        except:
            return jsonify({"code": "400", "status": "error"})
    
        
class SagaMovies(Resource):

    def get(self):
        try:
            data = request.headers
            saga = data['saga']
            query = "MATCH (m:MOVIE) -[:BELONGS_TO_SAGA]->(s:SAGA {Name: '%s'}) return m"%saga
            result = db.run(query)
            movies = []
            for record in result:
                res = record['m']
                movies.append({"Title": res['Title'], "image": res['Link_img'], "link": res['Link_trailer']})
            return jsonify(movies)
        except:
            return jsonify({"code": "400", "status": "error"})

class FanOf(Resource):
    def get(self):
        try:
            data = request.headers
            user = data['user']
            saga = data['saga']

            query = "RETURN EXISTS((:USER {Username: '%s'})-[:FAN_OF]->(:SAGA {Name: '%s'}))" %(user, saga)
            result = db.run(query).data()
            relationship_exists = False
            for keys in result[0]:
                relationship_exists = result[0][keys]

            return jsonify({"exists": relationship_exists})
        except:
            return jsonify({"code": "400", "status": "error"})

    def put(self):
        try:
            data = request.get_json()
            user = data['user']
            saga = data['saga']

            query = "RETURN EXISTS((:USER {Username: '%s'})-[:FAN_OF]->(:SAGA {Name: '%s'}))" %(user, saga)
            result = db.run(query).data()
            relationship_exists = False
            for keys in result[0]:
                relationship_exists = result[0][keys]
            
            if relationship_exists:
                query = "MATCH (u:USER {Username: '%s'})-[r:FAN_OF]->(:SAGA {Name: '%s'}) DELETE r"%(user, saga)
                db.run(query)
                return jsonify({"code": "200", "status": "deleted"})
            
            query = "MATCH (u:USER {Username: '%s'}), (s:SAGA {Name: '%s'}) MERGE (u)-[r:FAN_OF {timestamp: datetime()}]->(s) return r"%(user, saga)
            print(query)
            db.run(query)
            return jsonify({"code": "200", "status": "created"})
        
        except:
            return jsonify({"code": "400", "status": "error"})


