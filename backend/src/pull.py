# standard
import time
import json
import logging
from typing import BinaryIO, Any, Awaitable, List
from csv import DictReader
from functools import wraps
from io import TextIOWrapper
from requests import Session
from tqdm.notebook import tqdm
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# data science
import pandas as pd


# configurations
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger('chardet.charsetprober').setLevel(logging.INFO)

def set_default(obj):
    """
    sets ---> list hotfix
    """
    if isinstance(obj, set):
        return list(obj)
    raise TypeError

def get_request(session:Session, url:str, headers:dict) -> dict:
    return session.get(url, headers=headers)

def post_request(session:Session, url:str, body:dict, headers:dict) -> dict:
    # print("BODY: ", body) 
    return session.post(url, data=json.dumps(body), headers=headers) 

def setup_requests_session() -> Session:
    """
    setup a requests session with retry settings
    """    
    retry_plan = Retry(total=30, backoff_factor=.5,
                       status_forcelist=[429, 500, 502, 503, 504],
                       method_whitelist=["GET", "POST"])

    adapter = HTTPAdapter(max_retries=retry_plan)
    session = Session()
    session.mount('http://', adapter)
    session.mount('https://', adapter) 

    return session

def file_to_document_list(infile:BinaryIO, preprocessing_FUNC=None) -> List[Any]:
    """
    function returns a generator around a data file for memory handling

    converts a local bytes stream to a document (Dict)

    returns a list and not a generator since the data will need to be in memory outside the thread for insertion 
    """
    wrapper = TextIOWrapper(infile)
    reader = DictReader(wrapper) # reading in the file as a document
    rows = list()
    for row in reader: 

        if preprocessing_FUNC is not None:
            row = preprocessing_FUNC(row)
        
        rows.append(row)
    return rows

def power_nap(length:int) -> None:
    """
    give the server a break
    """
    with tqdm(total=length, desc="Power napping", leave=False) as pbar:
        for i in range(length):
            time.sleep(1)
            pbar.update(1)

def threader(call_function:object) -> List[Any]:
    """
    function for multithreading feed calls
    expects inputs:
        - thread_ct
        - nap_len
        - items

    (!): if you pass in call_function_kwargs, make sure you pass every individual keyword argument in order
    """
    @wraps(call_function)
    def wrapper(*args, **kwargs): # *args catches 'self'
        """
        wrap the call function in a threading pipeline
        """
        # setting defaults if not provided
        data = list()
        items = kwargs["items"] # this is required
        verbose = kwargs["verbose"] if "verbose" in kwargs else True
        nap_len = kwargs["nap_len"] if "nap_len" in kwargs else 60
        thread_ct = kwargs["thread_ct"] if "thread_ct" in kwargs else 10
        call_kwargs = list(kwargs["call_function_kwargs"].values()) if "call_function_kwargs" in kwargs else list()


        print(f"Running process with {thread_ct} threads and naps of length {nap_len}.")
        try: # running the calls
            chunks = [list(kwargs["items"])[index:index+thread_ct] for index in range(0, len(items), thread_ct)]
            for chunk in tqdm(chunks, total=len(chunks), desc="Thread pools", leave=False):
                EXECUTOR = ThreadPoolExecutor(max_workers=thread_ct)      
                thread_futures = [EXECUTOR.submit(call_function, *(list(args)+[item]+call_kwargs)) 
                                    for item in chunk]
                    
                
                # wait for each thread to finish then save the result to the memory structure
                thread_pool_progress = tqdm(total=len(thread_futures), leave=False, desc="Calls completed")
                for thread_future in as_completed(thread_futures):
                    thread_pool_progress.update(1)

                    # saving the output of this thread
                    if review_thread_results(thread_future, verbose): # if data was found
                        thread_output = thread_future.result()
                        data.append(thread_output["data"])


                thread_pool_progress.close()
                power_nap(nap_len) # giving the server a break
        
        except KeyboardInterrupt: # if the program stalls (happens sometimes on very long runs) and user wants to restart
            print("Run terminated early! Returning data pulled thus far.")

        
        return data
        
    return wrapper

def review_thread_results(thread:Awaitable, verbose:bool):
    """
    Function reviews multithreaded return results for review.
    """
    # Checking what duns returned data and which didn't (the latter will be rerun).
    try:
        _ = thread.result() # A way to test if the result was an exception or not.
        if _ is None: # if no data was returned
            return False
        # The data dimension will be printed by the API call function.
        return True # success        
    except Exception as e:
        if verbose:
            print("Error pulling data:")

        # Printing the error traceback if it is due to the code. 
        if type(e).__name__ != "AssertionError":
            traceback = e.__traceback__
            full_trace = []

            # Building up the full traceback to inspect.
            while traceback is not None:
                full_trace.append({"filename": traceback.tb_frame.f_code.co_filename,
                                    "name": traceback.tb_frame.f_code.co_name,
                                    "lineno": traceback.tb_lineno})
                traceback = traceback.tb_next

            if verbose:
                print(str({'type': type(e).__name__, 'message': str(e), 'trace': full_trace}))
        return False # failure