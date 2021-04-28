# Importing Data Manipulation packages:
import pandas as pd
import bonobo
import os
from datetime import date, timedelta, datetime
import pytz
import requests
import json

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
        The method queries the velkoz web api for the following datasets:

        - The existing wsb ticker freauency count dataset.
        - The r/wallstreetbets posts dataset
        - The NYSE and NASDAQ market index datasets

        The method uses the existing wsb ticker frequency count dataset to
        narrow down which wallstreetbets posts to query from the API.

        Yields:
            tuple: Containing all three of the dataframes queried from the
                REST API.
        """
        # Querying the existing wsb frequency counts:
        try:
            recent_wsb_freq = self.velkozz_con.get_wsb_ticker_counts().index[-1]
        except:
            recent_wsb_freq = None 

        logger.default_logger(f"API Queried, Most Recent Frequency Counts {recent_wsb_freq}")

        # If the most recent datetime was extracte, filter wsb posts by date:
        if recent_wsb_freq is not None:
            wsb_posts = self.velkozz_con.get_subreddit_data("wallstreetbets", start_date=recent_wsb_freq)
        
        # Else querying the entire wsb dataset:
        else:
            wsb_posts = self.velkozz_con.get_subreddit_data("wallstreetbets")

        # Querying the market index composition data:
        nyse_comp = self.velkozz_con.get_index_comp_data("nyse")
        nasdaq_comp = self.velkozz_con.get_index_comp_data("nasdaq")
        
        yield (wsb_posts, nyse_comp, nasdaq_comp)

    def transform(self, *args):
        """The method applies the main transformation to the wsb post datasets.
        
        The method applies the build_wsb_ticker_freq method to the wsb_posts dataframe
        pasted from the extraction method. This converts the dataframe of wallstreetbets
        posts to a dictionary of dates and frequency counts. This process is fully described
        in the documentation of the build_wsb_ticker_freq.

        The method converts the output from the build_freq_dicts method into a format that is 
        expected by the REST API:

        {date:{freq_dict}, date:{freq_dict}, date:{freq_dict}} is converted to
        
        [
            {'day':'yyyy-mm-dd', 'freq_counts': '{ticker frequencies}'},
            {'day':'yyyy-mm-dd', 'freq_counts': '{ticker frequencies}'},
            {'day':'yyyy-mm-dd', 'freq_counts': '{ticker frequencies}'}
        ]


        Yield:
            dict: A dictionary of dates and ticker frequency counts.
        """
        # Unpacking the argument tuples:
        wsb_posts = args[0]
        nyse_comp = args[1]
        nasdaq_comp = args[2]

        # Extracting the ticker lists from the market composition indicies:
        nyse_tickers = nyse_comp["symbol"].values.tolist()
        nasdaq_tickers = nasdaq_comp["symbol"].values.tolist()

        # Performing ticker frequency count extraction from wsb_posts:
        ticker_freq_count = build_wsb_ticker_freq(wsb_posts, nyse_tickers, nasdaq_tickers)
        
        # Converting the data into a format expected by the REST API:
        formatted_freq_dicts = [
            {"day": date, "freq_counts":freq_count} 
            for date, freq_count in ticker_freq_count.items()
        ]  

        yield formatted_freq_dicts

    def load(self, *args):
        """The method seralizes the dictionary of dates and ticker freqency counts into a
        JSON format and makes a POST request to the velkozz web api. 

        """
        # Constructing the endpoint for ticker frequency counts:
        ticker_counts_endpoints = f"{self.velkozz_con.finance_endpoint}/structured_quant/wsb_ticker_mentions/"

        # Unpacking argument tuples:
        formatted_freq_dicts = args[0]
        
        # Making POST request to the REST API:
        response = requests.post(
            ticker_counts_endpoints,
            headers={"Authorization": f"Token {self.token}"},
            json=formatted_freq_dicts
        )

        logger.default_logger(f"Made POST request to Velkoz Web API wsb Ticker Frequency Counts of length {len(formatted_freq_dicts)} with response {response.status_code}")