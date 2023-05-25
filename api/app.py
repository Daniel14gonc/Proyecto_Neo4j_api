from flask_restful import Api
from models import UserAuth, User, RandomMovie , WatchedMovie, SuggestedMovie, Movie, AllMovies, MyAccount, Saga, SagaMovies, FanOf, Admin
from flask import Flask
from flask_cors import CORS
  
# creating the flask app
app = Flask(__name__)
cors = CORS(app)
# creating an API object
api = Api(app)


# adding the defined resources along with their corresponding urls
api.add_resource(UserAuth, '/user-auth/')
api.add_resource(User, '/user/')
api.add_resource(RandomMovie, '/random-movie/')
api.add_resource(WatchedMovie, '/watched-movie/')
api.add_resource(SuggestedMovie, '/suggested-movie/')
api.add_resource(Movie, '/movie/')
api.add_resource(AllMovies, '/all-movies/')
api.add_resource(MyAccount, '/my-account/')
api.add_resource(Saga, '/saga/')
api.add_resource(SagaMovies, '/saga-movies/')
api.add_resource(Admin, '/admin/')
api.add_resource(FanOf, '/fan-of/')
# driver function
if __name__ == '__main__':
  
    app.run(debug = True)