"""
implements utility functions used by other scripts in this project module
"""
 

# standard
import os
import sys
import json
import zipfile
import operator
import multiprocessing
from functools import wraps
from itertools import repeat
from functools import reduce
from tqdm.notebook import tqdm
from collections import ChainMap
from multiprocessing import Pool
from datetime import datetime, timedelta
from collections import defaultdict as dd
from collections.abc import Mapping as MAPPING_STRUCTURE
from typing import Union, List, Any, Dict, Tuple, TypeVar

# database
import pymongo

# data science
import numpy as np
import pandas as pd 
import dask.dataframe as DaskDataFrame

# nlp
from nltk.corpus import stopwords


# configurations
PandasFrame = TypeVar('pd.DataFrame')
DaskFrame = TypeVar('DaskDataFrame')
Collection = TypeVar('pymongo.collection')


# handling json/dicts
def renest_document(doc, delimiter):
    """
    function restructures a document to a nested document based on a delimiter
    """
    renested_doc = dd(dict)
    for col, val in doc.items():
        renested_doc = update_value_at_key_chain(renested_doc, col.split(delimiter), 
                                                 val, instantiate=True)

    return renested_doc

def update_value_at_key_chain(mapping:dict, path:List[str], value:Any, 
                              instantiate:bool=False) -> dict:
    """
    function updates a single value at a list of keys
    """
    if instantiate: # creating the nested value key first 
        for idx, key in enumerate(path):
            if idx != len(path)-1: # if this isn't the last key
                
                try: # check if this key exists already (to avoid overwriting)
                    get_value_at_key_chain(mapping, path[:idx+1])
                except KeyError: # instantiate the sub-structure at this nested key
                    key_path_to_idx = "".join([r"['"+key+"']" 
                                                   for key in path[:idx+1]]) 
                    instantiate_command = "mapping"+key_path_to_idx+f" = dict()"
                    exec(instantiate_command)
                
                
    if len(path) > 1:
        try:
            reduce(operator.getitem, path[:-1], mapping)[path[-1]] = value
        except KeyError:
            print(f"missing key {path[-1]} for {mapping['contract']}")
            return mapping
    else:
        mapping[path[0]] = value

    return mapping

def get_value_at_key_chain(mapping:dict, path:List[str]) -> Any:
    """
    function gets a value at a key chain
    """
    if len(path) > 1:
        try:
            return reduce(operator.getitem, path, mapping)
        except KeyError:
            print(f"missing key {path[-1]} for {mapping['contract']}")
            return mapping

    elif len(path) == 1:
        return mapping[path[0]]
    else:
        return mapping

def flatten_from_node(node:Union[dict, str], node2children_data:dict, name='') -> Union[Dict[str, Dict], str, Dict[str, List]]:
    """
    Function is a helper function for FlattenJSON to call recursively when traversing .json data. 
    """
    if isinstance(node, MAPPING_STRUCTURE): # If the current node is a Dict. 
        for child in node: # For each child, this function is called recursively.
            node2children_data = flatten_from_node(node[child], node2children_data, name+child+'_')
    elif type(node) is list: # If the current data (node) is a list. 
        for i, child in enumerate(node): # For each item in the list, this function is recursively called again.
            node2children_data = flatten_from_node(child, node2children_data, name+str(i)+'_')
    else: # Otherwise, the value of the current tree node is recorded.
        node2children_data[name[:-1]] = node


    return node2children_data

def flatten_json(json_data:dict) -> dict:
    """
    Function Takes in a .json file and flattens contents to a Dict that still represents the nested
    structure through namings. 
    """
    node2children_data = flatten_from_node(json_data, dict()) # Starting the recursive .json traversal with an empty mapping to fill.
    
    return node2children_data 

def docs_to_frame(polish_function:object) -> object:
    """
    function flattens the data for each entry in a given list and returns a dataframe
    """
    def wrapper(*args, **kwargs):
        """
        wrapping any additional data processing around the flattened data
        """
        # flattening fpds documents
        flattened_data = list()
        trim_chain = kwargs["trim_chain"] if "trim_chain" in kwargs.keys() else list()
        for doc in tqdm(kwargs["docs"], total=len(list(kwargs["docs"])), desc="Flattening documents"): # made loop to monitor progress
            flattened_data.append(flatten_json(get_value_at_key_chain(doc, trim_chain)))
        
        # converting to a dataframe
        entries_df = pd.DataFrame(flattened_data)
        entries_df.reset_index(inplace=True, drop=True)

        if "processing_kwargs" in kwargs.keys():
            entries_df = polish_function(None, entries_df, kwargs["processing_kwargs"])

        return entries_df
    return wrapper

def df_to_json(df:PandasFrame):
    """
    reformat a dataframe to a json file
    """
    # preventing decoding errors
    for column in df:
        try:
            df[column] = df[column].str.encode('utf-8')
        except (AttributeError, TypeError):
            pass
    
    # mongo cannot have '.' in column names
    cols = list(df)
    renamed_cols = [c.replace(".", ">") for c in cols]
    df.rename(dict(zip(cols, renamed_cols)), axis=1, inplace=True)

    return json.loads(df.to_json(orient="records", force_ascii=False))

def unpack_dict_list(data:List[Dict[Any, Any]]) -> Dict[Any, Any]:
    """
    unpack a list of dictionaries

    (!): the keys within each should be unique to prevent data loss
    """
    return dict(ChainMap(*data))


# ddata handling
def add_column_source_tag(df:PandasFrame, tag:str) -> PandasFrame:
    """
    adds a tag name to each column in a data set

    also lowercases the rest of the column
    """
    df.columns = df.columns.map(lambda c: tag+"."+c.lower())


    return df

def chunk_date_range(start:datetime, end:datetime, chunk_size=30, date_format="%Y-%m-%d") -> List[Tuple[str]]:
    """
    function splits a date range into smaller chunks
    """
    chunk = timedelta(days=chunk_size)
    date_ranges = [(start, start+chunk)] if start+chunk < end else [(start, end)]

    if date_ranges[-1][-1] != end: # if more than two ranges are needed
        while True:
            last_chunk_end = date_ranges[-1][-1]
            new_chunk_end = last_chunk_end+chunk

            if new_chunk_end > end:
                date_ranges.append((last_chunk_end, end))
                break
            date_ranges.append((last_chunk_end, new_chunk_end))
        
    date_ranges = [(a_b_TUP[0].strftime(date_format), a_b_TUP[1].strftime(date_format)) for a_b_TUP in date_ranges]

    return date_ranges

def set_date_range(collection:Collection, 
                   start_at_most_recent:Union[str, bool], end_at_today:Union[str, bool]):
    """
    set_date_range sets a full date range
    """
    # setting the start date
    if start_at_most_recent==True:
        start_date = collection.find_one({}, sort=[('inserted', pymongo.DESCENDING)])["inserted"]
    else:
        start_date = datetime.strptime(start_at_most_recent, "%Y-%m-%d") # day one of FFATA 2006-09-26

    
    # setting the end date
    if end_at_today==True:
        date_ranges = chunk_date_range(start_date, datetime.today())
    else:
        date_ranges = chunk_date_range(start_date, datetime.strptime(end_at_today, "%Y-%m-%d"))


    continue_ = input(f"Enter 'C' to continue over range {date_ranges[0][0]} to {date_ranges[-1][-1]}: ")
    if continue_ == "C":
        return date_ranges
    else:
        sys.exit("user stopped")

def create_indexed_dask_frame(df:PandasFrame, idx_cols:List[str]) -> DaskFrame:
    """
    convert a pandas dataframe to a dask dataframe with specified indices 
    for efficiency in merging/grouping
    """
    df.dropna(subset=idx_cols, how="any", inplace=True)
    df["dask_idx"] = df[idx_cols].apply(lambda x: '-'.join(x), axis=1)

    df.set_index("dask_idx", inplace=True) 
    core_ct = multiprocessing.cpu_count()
    ddf = DaskDataFrame.from_pandas(df, npartitions=core_ct)

    return ddf

def list_to_frame(func:object) -> object:
    """
    return a dataframe given a list of dataframes
    """
    @wraps(func)
    def wrapper(data:List[pd.DataFrame], *args, **kwargs):
        data = func(*args, **kwargs)
        return pd.concat(data, sort=False)

    return wrapper