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
from sklearn.neighbors import KNeighborsClassifier, KDTree
import numpy as np
import asyncio
from math import log2
from sklearn.preprocessing import OneHotEncoder

model = KNeighborsClassifier(n_neighbors=3)
user_dictionary = {}
encoder = None

def train_KNN():
    query = """
        MATCH (u:USER)-[:WATCHED]->(m:MOVIE)-[:BELONGS_TO_SAGA]->(s:SAGA)
        WITH u, s, COUNT(*) AS peliculasSaga
        ORDER BY u, peliculasSaga DESC
        WITH u, COLLECT({saga: s, peliculasSaga: peliculasSaga})[0] AS sagaMasVista
        MATCH (u)-[:WATCHED]->(m)-[:OF_THE_GENRE]->(g:GENERO)
        WITH u, sagaMasVista, g, COUNT(*) AS peliculasGenero
        ORDER BY u, peliculasGenero DESC
        WITH u, sagaMasVista, COLLECT({genero: g, peliculasGenero: peliculasGenero})[0] AS generoMasVisto
        RETURN u.Username AS usuario, sagaMasVista.saga.Name AS saga, sagaMasVista.peliculasSaga AS peliculasSaga,
            generoMasVisto.genero.Tipo AS generoMasVisto, generoMasVisto.peliculasGenero AS peliculasGenero
    """
    result = db.run(query)
    arreglo = []
    i = 0
    for record in result:
        observation = []
        j = 0
        for caracteristic in record:
            if j == 0:
                global user_dictionary
                user_dictionary[i] = caracteristic
            if j == 2 or j == 4:
                caracteristic = int(caracteristic)
            observation.append(caracteristic)
            j += 1
        arreglo.append(observation)
        i += 1

    arreglo = np.array(arreglo)
    global encoder
    encoder = OneHotEncoder(handle_unknown='ignore')
    X_encoded = encoder.fit_transform(arreglo).toarray()
    # print(user_dictionary)
    global model
    model = KDTree(X_encoded)

train_KNN()

class UserAuth(Resource):
    
    def post(self):
        # try:
        data = request.get_json()
        username = data['username']
        password = data['password']
        query = "MATCH (n:USER {Username: '%s'}) return n"%(username)

        result = db.run(query)
        equal_password = False
        
        for record in result:
            user_node = record["n"]
            equal_password = user_node['Password'] == password
        train_KNN()
        return jsonify({"code": "200", "status": "authenticated"}) if equal_password else jsonify({"code": "401", "status": "error"})
        # except:
        #     return jsonify({"code": "400", "status": "error"})
        
    
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
        # try:
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
        # except:
        #     return jsonify({"code": "400", "status": "error"})
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
            
            query = "MERGE (u:USER {Username: '%s', Password: '%s', Age: %s, Registration_date: datetime(), Subscription_type: '%s'})"%(username, password, age, subscription)
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
        
        # try: 
            username = request.headers['user']
            
            query_tabla = """
            MATCH (u:USER {Username:'%s'})-[:WATCHED]->(m:MOVIE)-[:BELONGS_TO_SAGA]->(s:SAGA)
            WITH u, s, COUNT(*) AS peliculasSaga
            ORDER BY u, peliculasSaga DESC
            WITH u, COLLECT({saga: s, peliculasSaga: peliculasSaga})[0] AS sagaMasVista
            MATCH (u)-[:WATCHED]->(m)-[:OF_THE_GENRE]->(g:GENERO)
            WITH u, sagaMasVista, g, COUNT(*) AS peliculasGenero
            ORDER BY u, peliculasGenero DESC
            WITH u, sagaMasVista, COLLECT({genero: g, peliculasGenero: peliculasGenero})[0] AS generoMasVisto
            RETURN u.Username AS usuario, sagaMasVista.saga.Name AS saga, sagaMasVista.peliculasSaga AS peliculasSaga,
                generoMasVisto.genero.Tipo AS generoMasVisto, generoMasVisto.peliculasGenero AS peliculasGenero
            """%username
            
            result = db.run(query_tabla)
            records = []
            for record in result:
                records.append(str(record['usuario']))
                records.append(str(record['saga']))
                records.append(record['peliculasSaga'])
                records.append(str(record['generoMasVisto']))
                records.append(record['peliculasGenero'])
                print("redords:",record)
            
            if records:
                print(user_dictionary)
                global encoder
                print(records)
                X_encoded = encoder.transform([records]).toarray()
                distance, index = model.query(X=X_encoded, k=4, sort_results=True)
                distance = distance[0][1:]
                index = index[0][1:]
                users_list = [user_dictionary[i] for i in index]

                query_array =["""MATCH (u:USER {Username: '%s'})-[w:WATCHED]->(m1:MOVIE)
                                with m1 AS MovieTitle, AVG(w.Rating) AS AverageRating
                                order by AverageRating
                                limit 1
                                MATCH (MovieTitle)<-[:ACTED_IN]-(a:ACTOR)
                                MATCH (a)-[:ACTED_IN]->(m :MOVIE)
                                MATCH (u:USER)-[w:WATCHED]->(m:MOVIE)
                                with m , AVG(w.Rating) AS AverageRating2
                                order by AverageRating2
                                LIMIT 2
                                RETURN m""",
                                """MATCH (u:USER {Username: '%s'})-[w:WATCHED]->(m1:MOVIE)
                                with m1 AS MovieTitle, AVG(w.Rating) AS AverageRating
                                order by AverageRating
                                limit 1
                                MATCH (MovieTitle)-[:OF_THE_GENRE]->(a:GENERO)
                                MATCH (a)<-[:OF_THE_GENRE]-(m :MOVIE)
                                MATCH (u:USER)-[w:WATCHED]->(m:MOVIE)
                                with m , AVG(w.Rating) AS AverageRating2
                                order by AverageRating2
                                LIMIT 2
                                RETURN m""",
                                """MATCH (u:USER {Username: '%s'})-[r:WATCHED]->(m:MOVIE)
                                WITH m
                                ORDER BY r.Start_date DESC
                                LIMIT 2
                                return m"""]


                query = "MATCH (u:USER {Username: '%s'})-[:WATCHED]->(m:MOVIE) return m"
                movies = {}
                contador = 0
                for user in users_list:
                    print("usuario:",user)
                    result = db.run(query_array[contador]%user)
                    contador+=1
                    for record in result:
                        res = record['m']
                        movies[res['Title']] = {"Title": res['Title'], "image": res['Link_img'], "link": res['Link_trailer']}
                response = []
                for movie in movies:
                    response.append(movies[movie])

                return jsonify(response)
            else:
                query_count_movies = "MATCH (u:USER {Username:'%s'})-[:WATCHED]->(m:MOVIE) RETURN count(m) as c"%username
                result = db.run(query_count_movies)
                result = result.single()['c']
                print(result)
                
                
                query = "MATCH (m:MOVIE) return m"
                result = db.run(query)
                movies = []
                
                for record in result:
                    res = record['m']
                    movies.append({"Title": res['Title'], "image": res['Link_img'], "link": res['Link_trailer']})
                return jsonify(movies)
        # except:
        #     return jsonify({"code": "400", "status": "error"})

class Movie(Resource):

    def get(self):
        # try:
        data = request.headers
        username = data['username']
        movie = data['movie']

        

        query = "MATCH (u:USER {Username: '%s'})-[r:WATCHED]->(m:MOVIE {Title: '%s'}) return m, r"%(username, movie)
        
        result = db.run(query)
        record = result.single()
        
        try:
            relation = record['r']
            properties = relation._properties
            if "Rating" not in properties:
                properties['Rating'] = ""
            response = {'liked': properties['Liked'],'rating': properties['Rating'],'started':True}
        except:
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




class SagaActors(Resource):
    def get(self):
        # try:
        saga = request.headers['saga']
        query = "MATCH (a:ACTOR)-[:ACTED_IN]->(m:MOVIE)-[:BELONGS_TO_SAGA]->(s:SAGA {Name: '%s'}) return a.Actor_name"%saga
        result = db.run(query)
        actors = []
        for record in result:
            print(record['a.Actor_name'])
            name = record['a.Actor_name']
            actors.append(name)
        return jsonify(actors)
        # except:
        #     return jsonify({"code": "400", "status": "error"})
        
class SagaMovies(Resource):

    def get(self):
        try:
            query = "MATCH (s:SAGA) return s"
            result = db.run(query)
            sagas = []
            for record in result:
                res = record['s']
                sagas.append({"name": res['Name'], "image": res['saga_image']})
            print(sagas)
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
        # try:
        data = request.get_json()
        user = data['user']
        saga = data['saga']

        query = ''

        query = "RETURN EXISTS((:USER {Username: '%s'})-[:FAN_OF]->(:SAGA {Name: '%s'}))" %(user, saga)
        result = db.run(query).data()
        relationship_exists = False
        for keys in result[0]:
            relationship_exists = result[0][keys]
        
        if relationship_exists:
            items = ''
            if 'movie' in data or 'character' in data:
                if 'movie' in data:
                    items += "r.favorite_movie= '%s',"%data['movie']
                if 'character' in data:
                    items += "r.favorite_character= '%s',"%data['character']
                items = items[:-1]
                query = "MATCH (u:USER {Username: '%s'})-[r:FAN_OF]->(s:SAGA {Name: '%s'}) SET %s"%(user, saga, items)
                db.run(query)
                return jsonify({"code": "200", "status": "updated"})

            query = "MATCH (u:USER {Username: '%s'})-[r:FAN_OF]->(:SAGA {Name: '%s'}) DELETE r"%(user, saga)
            db.run(query)
            return jsonify({"code": "200", "status": "deleted"})
        
        query = "MATCH (u:USER {Username: '%s'}), (s:SAGA {Name: '%s'}) MERGE (u)-[r:FAN_OF {timestamp: datetime()}]->(s) return r"%(user, saga)
        db.run(query)
        return jsonify({"code": "200", "status": "created"})
        
        # except:
        #     return jsonify({"code": "400", "status": "error"})

class LikedMovies(Resource):
    def get(self):
        try:
            data = request.headers
            user = data['user']
            query = "MATCH (:USER {Username:'%s'})-[w:WATCHED]->(movie:MOVIE) WHERE w.Liked = true RETURN movie"%user
            movies = []
            result = db.run(query)
            for record in result:
                    res = record['movie']
                    movies.append({"Title": res['Title'], "image": res['Link_img'], "link": res['Link_trailer']})
            return jsonify(movies)
        except:
            return jsonify({"code": "400", "status": "error"})