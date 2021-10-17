"""
implements generic data source DataSource class

source-specific boths should inherit from this class (for organization)
"""


# standard 
import json
from datetime import datetime
from tqdm.notebook import tqdm
from pprint import pprint as pretty
from typing import Dict, Union, List, TypeVar

# data science
import pandas as pd
import dask.dataframe as DaskDataFrame

# module
from database import Database
from pull import setup_requests_session, get_request, post_request
from utils import add_column_source_tag, create_indexed_dask_frame, format_vendor_name, process_duns,\
                  get_value_at_key_chain, update_value_at_key_chain


# configurations
DaskFrame = TypeVar('DaskDataFrame')
PandasFrame = TypeVar('pd.DataFrame')
Collection = TypeVar('pymongo.collection')


class DataSource():
    def __init__(self, Database:object):
        self.mongo = Database

        # call setup
        self.session = setup_requests_session()
        self.headers = headers = {'Content-type':'application/json', 'Accept':'application/json', 
                                  'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}  


    # data pulling
    def send_get_request(self, url:str, parse=True) -> Union[dict, str]:
        response = get_request(self.session, url, self.headers)
        if parse:
            return json.loads(response.text)
        else:
            return response
    
    def send_post_request(self, url:str, body:dict) -> dict:
        response = post_request(self.session, url, body, self.headers)

        return json.loads(response.text)


    # data framing
    def polish_frame(self, df:PandasFrame, source_tag:str, dask_idx_cols:List[str]) -> Union[PandasFrame, DaskFrame]:
        """
        polish a dataframe and return a dask frame if desired
        """
        df = add_column_source_tag(df, source_tag)

        if len(dask_idx_cols) > 0:
            return create_indexed_dask_frame(df, dask_idx_cols)
        else:
            return df


    # database
    def process_collection(self, collection:Collection, filter_:dict=dict(), date_col2format:dict=dict(), 
                           duns_cols:List[str]=list(), name_cols:List[str]=list(), col_delimiter_split="~") -> List[dict]:
        """
        function is intended to be overridden in child classes
        
        if a feature to be processed is nested (common in fpds, etc.), use '@' to split up the items 
        passed in and set delimiter_split_cols to True
        """
        print(f"Note this function is splitting column names on delimiter '{col_delimiter_split}' to access nested features.")

        documents = list(collection.find(filter_))
        first = True
        for doc in tqdm(documents, desc="Processing documents"): # dates in mongo need to be UTC for usage!
        
            # duns code processing
            for duns_col in duns_cols:
                if first:
                    print(f"Processing DUNS column '{duns_col}'.")

                duns_col_chain = duns_col.split(col_delimiter_split)
                docs, _ = process_duns([doc], duns_col_chain, verbose=False)
                doc = docs[0] # process_duns returns a list of processed documents


            # name processing
            if len(name_cols) > 0:
                names = set()
                for name_col in name_cols:
                    if first:
                        print(f"Processing name column '{name_col}'.")
                    
                    name_col_chain = name_col.split(col_delimiter_split)
                    
                    names_under_feature = get_value_at_key_chain(doc, name_col_chain)
                    if isinstance(names_under_feature, str):
                        names.add(names_under_feature)
                    if isinstance(names_under_feature, list):
                        if len(names_under_feature) > 0:
                            names.update(set(names_under_feature))
                
                doc["names"] = list({format_vendor_name(name) for name in names})

            
            # date processing
            for date_col, format_ in date_col2format.items():
                if first:
                    print(f"Processing date column '{date_col}'.")
                
                date_col_chain = date_col.split(col_delimiter_split)
                formatted_date = datetime.strptime(get_value_at_key_chain(doc, date_col_chain), format_)
                update_value_at_key_chain(doc, date_col_chain, formatted_date)


            first = False

        return documents