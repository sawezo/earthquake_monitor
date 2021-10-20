
import os
import pymongo
from urllib.parse import quote_plus


class MongoDatabase:
    def __init__(self, db_name):
        # connect
        self.connect(db_name)
        
        # select db/coll
        self.db = self.client[db_name]
        
    def connect(self, db_name):
        MONGO_USER = os.environ['MONGO_USER']
        MONGO_PASSWORD = os.environ['MONGO_PASSWORD']
        URI = f"mongodb://{MONGO_USER}:{quote_plus(MONGO_PASSWORD)}@mongo_db:27017/{db_name}?authSource=admin"
        self.client = pymongo.MongoClient(URI)

    def select_collection(self, collection_name):
        self.collection = self.db[collection_name]
    
    def geo_index(self):
        # setup geoindex
        self.collection.create_index([("geometry", pymongo.GEOSPHERE)])


#     # query test
# data = mongo_db.collection.find(
# {
#    "geometry": { # the geoindex
#      "$near": {
#        "$geometry": {
#           "type": "Point" ,
#           "coordinates": [64, 28]
#        },
#      }
#    }
# }).limit(1)
# for i in data:
#     print(i)