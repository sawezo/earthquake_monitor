# standard
# import time
import json
# import logging
# from typing import BinaryIO, Any, Awaitable, List
# from csv import DictReader
# from functools import wraps
# from io import TextIOWrapper
from requests import Session
# from tqdm.notebook import tqdm
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
# from concurrent.futures import ThreadPoolExecutor, as_completed

# data science
# import pandas as pd


global DEFAULT_HEADERS
DEFAULT_HEADERS = {"Content-type":"application/json", 
                   "Accept":"application/json", 
                   "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0"}


class Puller():
    """
    pulls data from API
    """
    def __init__(self):
        self.setup()

    def setup(self):
        """
        setup a requests session with retry settings
        """    
        retry_plan = Retry(total=30, backoff_factor=.5,
                           status_forcelist=[429, 500, 502, 503, 504],
                           method_whitelist=["GET"])

        adapter = HTTPAdapter(max_retries=retry_plan)
        session = Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        self.session = session

    def get(self, url, headers=DEFAULT_HEADERS):
        """
        GET request
        """
        response = self.session.get(url, headers=headers)
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            print(f'error pulling data (response {response.status_code}) | URL: {url}')