# Importing Data Manipulation packages:
import pandas as pd
import bonobo
import os
from datetime import date, timedelta, datetime
import pytz
import requests


# Importing internal modules:
from vdeveloper_api.velkozz_pipelines.core_objects import Pipeline
from vdeveloper_api.velkozz_pipelines.utils import logger

# Python API Wrappers:
from vdeveloper_api.velkozz_pywrapper.query_api.velkozz_api import VelkozzAPI
from vdeveloper_api.velkozz_pywrapper.api_utils.quant_data_utils.social_media_utils import build_wsb_ticker_freq

class WSBTickerFrequencyPipeline(Pipeline):
    """The object contains all of the methods usd to build a ETL pipeline
    for frequency of ticker mentions in wallstreetbets reddit posts.

    It inherits from the Pipeline API object with allows the extract, transform and load 
    methods to be overwritten but the graph creation and pipeline execution methods to be 
    inherited. 
    
    The method queries wallstreetbets posts from the Velkozz Web API, extracts 
    unique elements, extracts the frequnecy counts for ticker mentions for each post and
    writes said frequency counts to the REST API.

    The pipeline queries the Velkozz REST API for ticker symbols that are used to build
    the frequecy counts. These ticker symbols are queried from the NYSE and NASDAQ market
    composition datasets.

    """
    def __init__(self, **kwargs):
        self.token = kwargs.get('token')
        self.url = kwargs.get("url")

        # Creating connection to the REST API:
        if self.url is None:
            self.velkozz_con = VelkozzAPI(token=self.token)
        else:
            self.velkozz_con = VelkozzAPI(token=self.token, url=self.url)
        
        logger.default_logger("WallStreetBets Ticker Frequency Counts Pipeline Initalized")

        # Execuring all of the ETL functions mapped in the graph:
        self.execute_pipeline()

    def extract(self):
        """
        - Query the existing ticker frequencies posts.
        - Query the API for a list of wsb posts. 
        - Query the API for market index composition data.
        """
        # Querying the existing wsb frequency counts:
        # TODO: Add logic to extract most recent wsb frequency count:
        recent_wsb_freq = None

        # Querying wallstreetbets posts from API:
        wsb_posts = self.velkozz_con.get_subreddit_data("wallstreetbets")

        # Querying the market index composition data:
        nyse_comp = self.velkozz_con.get_index_comp_data("nyse")
        nasdaq_comp = self.velkozz_con.get_index_comp_data("nasdaq")
        
        yield (wsb_posts, nyse_comp, nasdaq_comp)

    def transform(self, *args):
        """
        - Apply the build_wsb_ticker_freq function to the data.
        """
        # Unpacking the argument tuples:
        wsb_posts = args[0]
        nyse_comp = args[1]
        nasdaq_comp = args[2]

        # Extracting the ticker lists from the market composition indicies:
        nyse_tickers = nyse_comp["symbol"].values.tolist()
        nasdaq_tickers = nasdaq_comp["symbol"].values.tolist()

        # Performing ticker frequency count extraction from wsb_posts:
        ticker_freq_count = build_wsb_ticker_freq(wsb_posts, [nyse_tickers, nasdaq_tickers])
        print(ticker_freq_count)

    def load(self, *args):
        """
        - Seralize the data to JSON format and make POST request to the Web API.
        """
        pass

WSBTickerFrequencyPipeline(token="21128e1acfba7afb5656d30dbd24ebd892aa2049", url="http://localhost:8001")