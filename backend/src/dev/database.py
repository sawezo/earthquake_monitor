"""
implements general database functionality

this uses mongo db (NoSQL). there are a few main reasons I went with mongo instead of something else:
    - we don't have a set schema and will likely change a lot of this code/how data is stored
        - also just a lot nicer to keep complex json objects (especially from fpds) instead of flattening it all out
    - timescale db may be quicker for temporal documents, but we don't need a perfect db, and I already know mongo/its neo4j connector that I know


why I went with a single time series entry per document: 
    - there are some problems with storing multiple entries per document:
        - a single document is limited to a certain size (currently 16 MB); this limits how many entries can be stored in a single document
        - as more entries are added to a document, the entire document (and time series) will needlessly be deleted and reallocated to a larger piece of memory
        - queries on sub-documents are limited compared to queries on regular documents
        - documents with very flat structures (like one sub-document for each second) are not performant
        - the built-in map-reduce does not work as well on sub-documents
    - overall because it's the easiest option to implement quickly noting this database may only be a temporary solution
"""


# standard
import os
import sys
from urllib.parse import quote_plus
from functools import wraps
from datetime import datetime
from tqdm.notebook import tqdm
from pprint import pprint as pretty
from typing import Dict, List, Tuple, TypeVar, Union

# database
import pymongo


# configurations
Database = TypeVar('pymongo_database')
Collection = TypeVar('pymongo_collection')

# globals
global MONGO_USER 
MONGO_USER = os.environ['MONGO_ADMIN_USER']

global MONGO_PASSWORD 
MONGO_PASSWORD = os.environ['MONGO_ADMIN_PASSWORD']


class Database():
    def __init__(self, project_run_tag):
        connection_str = f"mongodb://{MONGO_USER}:{quote_plus(MONGO_PASSWORD)}@localhost:29017/admin?authSource=admin"
        self.client = pymongo.MongoClient(connection_str)
        self.db = self.client[project_run_tag]
        self.collection = None
        self.initial_database_name = project_run_tag

    def print_info(self) -> Dict[Database, List[Collection]]:
        return dict((db, [collection for collection in self.client[db].collection_names()])
                      for db in self.client.database_names())

    def _update_index_name_map(self) -> None:
        """
        make a mapping to make it easier to query index values
        """
        self.index_name2feature_chain = dict(zip(self.collection.index_information().keys(),
                                                 [val["key"][0][0] for val in self.collection.index_information().values()]))

    def select_db(self, db_name:str) -> None:
        """
        selecting a database collection
        """
        # creating a collection
        self.db = self.client[db_name]

        self.collection = "No collection selected (call .select_collection(coll_name))."

    def select_collection(self, collection_name) -> Collection:
        """
        selecting a database collection
        """
        # creating a collection
        self.collection = self.db[collection_name]
        self._update_index_name_map() # index name mapping

        print(f"switched to db.collection: {self.db.name}.{self.collection.name}")
        return self.collection

    @staticmethod
    def _general_insertion_preprocessing(documents:List[dict]) -> List[dict]:
        """
        (!): if this code is changed, additional data should not be entered into an existing collection! 
        this is because then some documents would have been processed differently prior to insertion

        function performs preprocessing for data prior to saving to any collection in a database

        data should be completely raw prior to running this function (use `process_documents` for 
        additional processing). This is to ensure there is always a raw data store so that data doesn't need to be called for again.
        
        this functionality should strictly be run when inserting new data into the database
        """
        for doc in tqdm(documents, desc="Pre-insertion formatting"): # dates in mongo need to be UTC for usage!
            
            # adding an insertion date
            doc["inserted"] = datetime.utcnow()


        return documents

    def insert_data(self, documents:List[dict], indexing_function:object=None) -> None:
        """
        save_data_to_db saving data to the selected database collection
        """
        if self.collection is None:
            collection_name = input("Enter the collection name: ")
            self.select_collection(collection_name)
        print(f"Saving {len(documents)} documents to collection '{self.collection.name}'.")


        documents = self._general_insertion_preprocessing(documents)
        if len(documents) > 0:
            self.collection.insert_many(documents)

            if indexing_function is not None:
                # updating indices as necessary (new content type or first time data added to collection)
                indexing_function(self.collection)
                self._update_index_name_map()                                                 
        else:
            print("No data to insert!")

    def write_processed_documents(self, raw_db_name:str, raw_collection_name:str, 
                                  processing_function:object, indexing_function:object, 
                                  switch_collection=True, **kwargs) -> Union[None, Collection]:
        """
        function processes data in the database, then writes it to a new collection

        the new collection name is the old name + "processed"

        can pass argument to automatically switch to processed collection
        
        function is made to reinstantiate the processed collection every time you run it, to avoid 
        data inconsistencies if this function is updated (this is why the raw collection is helpful...)
        """
        # making sure we are pointing to the correct db/collection to process
        self.select_db(raw_db_name)
        self.select_collection(raw_collection_name)

        # check before dropping any existing processed collection
        processed_collection_name = raw_collection_name+"_processed"
        if processed_collection_name in self.db.list_collection_names():
            existing_doc_ct = self.db[processed_collection_name].count()
            continuez = input(f"(!): This will erase the existing {str(processed_collection_name)} ({existing_doc_ct} documents). 'C' to continue. ")
            if continuez != "C":
                sys.exit("user stopped")
            self.db[processed_collection_name].drop() # dropping the existing processed collection
        

        # processing the data in the currently selected collection
        documents = processing_function(self, **kwargs)
        
        
        # saving the processed data to a new collection in the current database 
        self.select_db(self.initial_database_name)
        self.select_collection(processed_collection_name)
        self.insert_data(documents, indexing_function) # inserting the processed data into a new collection
        
        
        # staying on the processed collection if desired
        if switch_collection: 
            # already changed over to it above
            print(f"\nSwitching to collection '{processed_collection_name}'")
        else: # going back to the raw data collection
            self.select_collection(raw_collection_name)            

    def index_collection(self, collection_name, index2feature_chain__kwargs:Dict[int, Tuple[List[str], dict]], 
                         verbose=True) -> None:
        """
        index_collection index a database collection
        """
        # adding any new indices to this collection
        collection = self.db[collection_name]
        for index, feature_chain__kwargs in index2feature_chain__kwargs.items():    
            if index in list(collection.index_information().keys()):
                continue # if this index has already been created, skip it
                
            collection.create_index(feature_chain__kwargs[0], **feature_chain__kwargs[1], name=index)

        if verbose:
            print(f"\nThe '{collection.name}' collection contains the following indices: ")
            print("\n")
            pretty(collection.index_information())

    def _iterate_mongo_collections(func:object) -> object:
        """
        function iterates through all mongo databases and collections
        """
        @wraps(func)
        def wrapper(*args): # catch self (project reference)
            for db in args[0].print_info().keys():
                if db not in ["local", "config"]: 
                    args[0].select_db(db)

                    for coll in list(args[0].db.list_collections()):
                        if coll["name"] not in ["system.version"]:
                            args[0].select_collection(coll["name"])
                            func(args[0])
        return wrapper

    def backup_collection(self, db_name:str, coll_name:str) -> None:
        """
        store a backup of all mongo databases
        """
        print(f"Backing up collection '{coll_name}' in db '{db_name}'.")
        os.system(f"mongodump -o ../../data/mongo_backup/ --db {db_name} --collection {coll_name} --authenticationDatabase admin -u root -p {quote_plus(MONGO_PASSWORD)}")
        
    def _delete_collection(self, collection_name:str) -> None:
        """
        drop a collection from the database
        """
        continue_ = input(f"Press 'C' to confirm delete collection {collection_name}: ")
        if continue_ == "C":      
            self.backup_collection(self.db.name, collection_name)
            self.select_collection(collection_name)
            self.collection.drop()
        else:
            print("not dropping this collection")

    @_iterate_mongo_collections
    def wipe_mongo(self) -> None:
        """
        function wipes a mongo database

        for safety, it first saves out a copy of all data
        
        (!): intended for use in development only!
        """
        self._delete_collection(self.collection.name)

    @_iterate_mongo_collections
    def backup_mongo(self) -> None:
        """
        function wipes a mongo database

        for safety, it first saves out a copy of all data
        
        (!): intended for use in development only!
        """
        self.backup_collection(self.db.name, self.collection.name)

        # moving the dump directory to a data folder
        os.system("mv dump ../data/mongo_backup")

    def cleanup(self) -> None:
        """
        shut down the database when not in use
        """
        self.backup_mongo() 
#         os.system("brew services stop mongodb-community@4.4")
        # brew services stp mongodb/brew/mongodb-community
