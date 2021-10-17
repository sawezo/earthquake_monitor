"""
implements process run 'project' container
"""


# standard
import os
import sys
import json
import logging
from functools import wraps
from datetime import datetime
from tqdm.notebook import tqdm
from networkx.readwrite import json_graph
from typing import Iterator, List, Dict, Union, Tuple, TypeVar

# data science
import numpy as np
import pandas as pd 
import dask.array as da
import dask.dataframe as DaskDataFrame

# database
import pymongo

# module
from database import Database
from utils import renest_document, get_value_at_key_chain, create_indexed_dask_frame, \
                  add_column_source_tag, df_to_json


# configurations
PandasSeries = TypeVar('pd.Series')
PandasFrame = TypeVar('pd.DataFrame')
DaskFrame = TypeVar('DaskDataFrame')
logging.basicConfig(level=logging.DEBUG)

# globals
global BULK_STORE_DB
BULK_STORE_DB = "bulk_store"


class Project:
    def __init__(self, db_name:str):
        # organization
        self.timestamp = datetime.now().strftime("%m_%d_%Y")
        self.project_run_tag = db_name+"_"+self.timestamp # will reference when switching to/from admin db


        # database
        self.mongo = Database(self.project_run_tag)
        print(f"created database for project run: '{self.project_run_tag}'")


    # processing
    def get_fpdsfsrs_duns_pool(self) -> List[str]:
        """
        getting a pool of all duns to pull data for 
        """
        duns_pool = set()
        print(f"finding all unique duns in (processed) fsrs and fpds collections for database '{self.mongo.db.name}'")

        # fsrs duns
        fsrs_duns_cols = ['prime_awardee_duns', 'prime_awardee_parent_duns', 'subawardee_duns', 'subawardee_parent_duns']
        self.mongo.select_collection("fsrs_processed")
        for duns_col in fsrs_duns_cols:
            duns_pool.update(self.mongo.collection.find().distinct(duns_col))

        # fpds duns
        fpds_duns_cols = ["content.ns1:award.ns1:vendor.ns1:vendorSiteDetails.ns1:entityIdentifiers.ns1:vendorDUNSInformation.ns1:DUNSNumber", 
                          "content.ns1:award.ns1:vendor.ns1:vendorSiteDetails.ns1:entityIdentifiers.ns1:vendorDUNSInformation.ns1:globalParentDUNSNumber"]
        self.mongo.select_collection("fpds_processed")
        for duns_col in fpds_duns_cols:
            duns_pool.update(self.mongo.collection.find().distinct(duns_col))
        
        
        return [d for d in duns_pool if d != ""]

    # def delete_duns_from_admin_vendors(self, duns_to_delete:List[str]) -> None:
    #     """
    #     delete duns from the vendors collection of the admin database
        
    #     useful if you want to truly update data pulled for a set of vendors
    #     """
    #     print("deleting the following DUNS from the 'vendors' collection of the 'admin' database:")
    #     print(duns_to_delete)

    #     self.mongo.select_db("admin") 
    #     self.mongo.select_collection("vendors")
    #     print("original admin vendors data collection count: ", self.mongo.collection.count())
    #     for duns in duns_to_delete:
    #         try:
    #             self.mongo.collection.delete_one({'duns':duns})
    #         except: 
    #             print(f"duns {duns} not found")
    #     print("new admin vendors collection data count", self.mongo.collection.count())


    #     print(f"switching back to the 'vendors' collection of database '{self.mongo.db.name}'")
    #     self.mongo.select_db(self.project_run_tag) 
    #     self.mongo.select_collection("vendors")

    def filter_existing_duns(self, duns_set:set, include_duns_not_found:bool):
        """
        filter a duns list to duns that are not already in the 'vendors' 
        collection of the storage database 
        """
        print("switching to the 'vendors' collection of the storage database to see what we already have")
        # get a list of duns we dont have data for
        self.mongo.select_db(BULK_STORE_DB) 
        self.mongo.select_collection("vendors")
        
        
        # duns we already have data for
        existing_duns = {d["duns"] for d in self.mongo.collection.find({'duns':{"$in":duns_set}})}
        
        
        # if we want to include DUNS we have called for before that we did not find a hash id for
        if include_duns_not_found: 
            self.mongo.select_collection("duns_not_found")
            duns_found_empty_before = {d["duns"] for d in self.mongo.collection.find()}
            existing_duns.update(duns_found_empty_before)


        # switching back to the initial project run db/vendor collection
        print(f"switching back to the 'vendors' collection of database '{self.project_run_tag}'")
        self.mongo.select_db(self.project_run_tag)
        self.mongo.select_collection("vendors")
        
        return {d for d in duns_set if d not in existing_duns}

    def trace_vendor_hierarchy_links(hash_id2doc:Dict[str, dict]) -> Dict[str, dict]:
        """
        formatting duns relationships for the knowledge graph
        
        this info is done here instead of during calls to avoid missing information over different threads
        
        only have access to parent information outside function 'pull_vendor_data', 
        so children are built by reversing parent relations
        
        (!): the above means this assumes reversed parent relations cover all children relationships
        """
        duns2relation2linked_duns = dict()
        for doc in tqdm(hash_id2doc.values(), desc="Building up more links from USA Spending data."):

            # adding parent relations
            doc_parents = {link["parent_duns"] for link in doc["parents"]} 

            if doc["duns"] in duns2relation2linked_duns:
                duns2relation2linked_duns[doc["duns"]]["vendor_has_parent"].update(doc_parents)  
            else:
                duns2relation2linked_duns[doc["duns"]] = {"vendor_has_child":set(), "vendor_has_parent":doc_parents}


            # adding child relations from each parent to this duns
            # (!): this assumes they are all covered by reversing parent connections
            for parent in doc_parents:
                if parent in duns2relation2linked_duns:
                    duns2relation2linked_duns[parent]["vendor_has_child"].update({doc["duns"],})
                else:
                    duns2relation2linked_duns[parent] = {"vendor_has_child":{doc["duns"],}, "vendor_has_parent":set()}


        return duns2relation2linked_duns

    def add_prime_and_sub_flag(self, workbook_df:PandasFrame) -> PandasFrame:
        """
        adds a flag signifying if a contractor is both a prime and a sub on a 
        contract or program ('group_over')
        """
        # grouping will be quicker; note program_x==program_y (likewise for 'contract_x/y')
        idx_cols = ["entity", "program", "contract_x"]
        entity_contract_presence_df = workbook_df.groupby(idx_cols)["vendor_to_contract_how"].unique().reset_index()

        prime_FUNC = lambda x: True if "vendor_is_prime_on_contract" in x else False
        sub_FUNC = lambda x: True if "vendor_is_subawardee_on_contract" in x else False

        entity_contract_presence_df["META.prime_on_contract"] = entity_contract_presence_df.vendor_to_contract_how.apply(prime_FUNC)
        entity_contract_presence_df["META.sub_on_contract"] = entity_contract_presence_df.vendor_to_contract_how.apply(sub_FUNC)


        # combining the findings into a new formatted feature
        def combine_prime_sub_status(row:PandasSeries) -> str:
            """
            helper function for formatting contract prime/sub status for rows over a dataframe
            """
            is_prime = row["META.prime_on_contract"]
            is_sub = row["META.sub_on_contract"]
            
            if is_prime and is_sub:
                return "prime and sub"
            elif is_prime:
                return "only prime"
            elif is_sub:
                return "only sub"
            else:
                return "not either"

        entity_contract_presence_df["META.prime_and_sub_on_contract"] = entity_contract_presence_df.apply(combine_prime_sub_status, axis=1)
        print("unique entity-program-contract prime/sub statuses:\n", entity_contract_presence_df["META.prime_and_sub_on_contract"].value_counts())


        return self.inner_df_daskmerge_to_df(workbook_df, entity_contract_presence_df, idx_cols, idx_cols)

    def save_connection_workbook_to_database(self, df):
        """
        save connection workbook to 'workbook' collection of current database
        """
        # conversion to json
        workbook_json = df_to_json(df)


        # re-nesting document elements on the '>' delimiter
        renested_docs = []
        for doc in tqdm(workbook_json, desc="renesting"):
            renested_docs.append(renest_document(doc, ">"))
        
    
        # saving the restructured data
        self.mongo.select_collection("workbook")
        self.mongo.insert_data(workbook_json)
        
        
        # indexing the collection
        index2featureTUPkwargs = {"insertion_date_idx":("inserted", {"unique":False}),
                                  "entity_idx":("entity", {"unique":False}), 
                                  "program_number_idx":("program_number", {"unique":False}), 
                                  "program_name_idx":("program_name", {"unique":False}), 
                                  "contract_idx":("contract", {"unique":False}), 
                                  "duns_pair_idx":("duns_pair", {"unique":False})}
            
        self.mongo.index_collection(self.mongo.collection.name, index2featureTUPkwargs)

    def save_graph_to_db(self, G):
        """
        save the nodes and links of a directed knowledge graph to mongo
        """
        # save the graph to mongo
        graph_data = json_graph.node_link_data(G)
        for node in tqdm(graph_data["nodes"], desc="Formatting nodes for mongo"):
            for key in node: # no sets in mongo
                if isinstance(node[key], set):
                    node[key] = list(node[key])

        for key in ["nodes", "links"]:
            self.mongo.select_collection(f"graph_{key}")
            self.mongo.insert_data(graph_data[key])


    # merging
    def outer_fsrs_fpds(self, fsrs_df:DaskFrame, fpds_df:DaskFrame) -> PandasFrame:
        """
        merging fpds and fsrs data on contract number and (optionally) duns numbers
        """
        # all rows of FSRS data and as many matching FPDS BASE rows as possible
        fsrs_fpds_ddf = fsrs_df.merge(fpds_df, how="outer", on="dask_idx", indicator=True)
        fsrs_fpds_df = fsrs_fpds_ddf.compute()
        
        print(fsrs_fpds_df["_merge"].value_counts())

        # adding a source feature
        fsrs_fpds_df.rename({"_merge":"fpds_fsrs_presence"}, inplace=True, axis=1)
        fsrs_fpds_df["fpds_fsrs_presence"] = fsrs_fpds_df["fpds_fsrs_presence"].astype(str) # from category
        fsrs_fpds_df.loc[fsrs_fpds_df.fpds_fsrs_presence=="left_only", "fpds_fsrs_presence"] = "fsrs"
        fsrs_fpds_df.loc[fsrs_fpds_df.fpds_fsrs_presence=="right_only", "fpds_fsrs_presence"] = "fpds"
        fsrs_fpds_df.loc[fsrs_fpds_df.fpds_fsrs_presence=="both", "fpds_fsrs_presence"] = "fsrs|fpds"

        return fsrs_fpds_df

    def left_contracts_vendors(self, df:PandasFrame, vendor_ddf:DaskFrame, duns_col2duns_type:dict) -> pd.DataFrame:
        """
        left merge vendor data to the contract data on duns
        """
        first_column = True
        for col, duns_type in tqdm(duns_col2duns_type.items(), leave=False, desc="Merging duns types"):
            # setting merge indices before converting the dataframe to a dask frame for quicker merging of massive data
            if first_column:
                ddf = create_indexed_dask_frame(df, [col])
                first_column = False
            else:
                ddf = create_indexed_dask_frame(merged_df, [col])


            # merging
            merged_ddf = ddf.merge(vendor_ddf, on="dask_idx", how="left")


            # adding the duns type to the column
            new_vendor_cols = [c for c in list(merged_ddf) if "SAM" in c]

            # lowercase to avoid pulling these columns on next pass
            merged_ddf = merged_ddf.rename(columns={old:".".join(["sam", duns_type, old.split(".")[1]]) 
                                                                    for old in new_vendor_cols}) 


            merged_df = merged_ddf.compute()

            
        return merged_df

    def inner_fpds_fsrs_vendors(self, fpds_only_workbook_df:PandasFrame, fsrs_workbook_df:PandasFrame, contracts_df:PandasFrame) -> PandasFrame:
        """
        create the final workbook frame by combining the vendor frames (vertical stack) 
        and merging with contract data
        """
        # stacking fpds and fsrs contract/vendor frames
        print("stacking fpds/fsrs")
        fsrs_fpds_vendors_df = pd.concat([fpds_only_workbook_df, fsrs_workbook_df], axis=0)


        # converting to dask frames for a merge with 
        print("merging with contract data")
        workbook_ddf = create_indexed_dask_frame(fsrs_fpds_vendors_df, ["contract"])
        contracts_ddf = create_indexed_dask_frame(contracts_df, ["contract"])

        workbook_ddf = contracts_ddf.merge(workbook_ddf, on="dask_idx")


        return workbook_ddf.compute()

    def inner_df_daskmerge_to_df(self, df1:PandasFrame, df2:PandasFrame, df1_idx_cols:list, df2_idx_cols:list) -> pd.DataFrame:
        """
        function merges a two dataframes after converting them to dask frames, then returns a dataframe
        """
        # indexing and converting dataframes to dask arrays for more efficient merges
        ddf1 = create_indexed_dask_frame(df1, df1_idx_cols)
        ddf2 = create_indexed_dask_frame(df2, df2_idx_cols)

        merged_ddf = ddf1.merge(ddf2, how="inner", on="dask_idx")

        return merged_ddf.compute()