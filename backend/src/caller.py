import json
import logging

from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import pandas as pd


class Caller:
    def __init__(self):
        retry_plan = Retry(total=2, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_plan)
        
        self.session = Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        self.headers = {"Content-type":"application/json", 
                        "Accept":"application/json", 
                        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0"}       
    
    def get(self, url):
        response = self.session.get(url, headers=self.headers)
        
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            print(f'error pulling data (response {response.status_code})')