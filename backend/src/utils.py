from typing import List
import pandas as pd
from collections.abc import Mapping as MAPPING


def flatten_from_node(node:dict, node2children_data:dict, name='', spacer=">") -> dict:
    """
    Function is a helper function for FlattenJSON to call recursively when traversing .json data.
    """
    if isinstance(node, MAPPING): # If the current node is a dictionary.
        for child in node: # For each child, this function is called recursively.
            node2children_data = flatten_from_node(node[child], node2children_data, name+child+spacer)
    elif type(node) is list: # If the current data (node) is a list.
        for i, child in enumerate(node): # For each item in the list, this function is recursively called again.
            node2children_data = flatten_from_node(child, node2children_data, name+str(i)+spacer)
    else: # Otherwise, the value of the current tree node is recorded.
        node2children_data[name[:-1]] = node


    return node2children_data

def flatten_json(json_data:dict) -> dict:
    """
    Function Takes in a .json file and flattens contents to a dictionary that still represents the nested
    structure through namings.
    """
    node2children_data = flatten_from_node(json_data, dict()) # Starting the recursive .json traversal with an empty mapping to fill.
   
    return node2children_data

def frame_data(features:List[dict], keep2rename):
    quakes = [flatten_json(q) for q in features]
    df =  pd.DataFrame.from_records(quakes)

    df.rename(keep2rename, axis=1, inplace=True)

    df = df[list(keep2rename.values())]  
    return df