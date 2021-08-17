# Importing Data Manipulation packages:
import pandas as pd
import bonobo
import os
from datetime import date, timedelta, datetime
import pytz
import requests
import json
import itertools
from datetime import datetime
import collections

# Importing internal modules:
from vdeveloper_api.velkozz_pipelines.core_objects import Pipeline
from vdeveloper_api.velkozz_pipelines.utils import logger

# Python API Wrappers:
from vdeveloper_api.velkozz_pywrapper.query_api.velkozz_api import VelkozzAPI
#from vdeveloper_api.velkozz_pywrapper.api_utils.quant_data_utils.social_media_utils import build_wsb_ticker_freq

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
        
        # Initalizing the parent Pipeline object:
        super(WSBTickerFrequencyPipeline, self).__init__(**kwargs)

        # Creating connection to the REST API:
        if self.web_api_url is None:
            self.velkozz_con = VelkozzAPI(token=self.token)
        else:
            self.velkozz_con = VelkozzAPI(token=self.token, url=self.web_api_url)
        
        self.logger.info("WallStreetBets Ticker Frequency Counts Pipeline Initalized", "reddit_quant", "pipeline", 200) 
        
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
            self.logger.info(f"Request made to Velkozz API for most recent ticker count. Extracted {recent_wsb_freq}", "reddit_quant", "pipeline", 200) 
        except Exception as e:
            self.logger.warning(f"Request made to Velkozz REST API failed. Setting recent_ticker_freq to None w Error: {e} ", "reddit_quant", "pipeline", 301) 
            recent_wsb_freq = None 

        # If the most recent datetime was extracte, filter wsb posts by date:
        try:
            if recent_wsb_freq is not None:
                wsb_posts = self.velkozz_con.get_subreddit_data("wallstreetbets", start_date=recent_wsb_freq)
                self.logger.info(f"Most recent date was extracted from the Velkozz REST API. {len(wsb_posts)} Reddit posts were queried from the REST API ", "reddit_quant", "pipeline", 200) 

            # Else querying the entire wsb dataset:
            else:
                wsb_posts = self.velkozz_con.get_subreddit_data("wallstreetbets")
                self.logger.info(f"No date was extracted from the REST API, querying all {len(wsb_posts)} reddit post from the REST API", "reddit_quant", "pipeline", 200) 

        except Exception as e:
            self.warning(f"Error in querying subreddit data from REST API. Pipeline Exit w/ {e}", "reddit_quant", "pipeline", 400) 
            return

        # Querying the market index composition data:
        try:
            nyse_comp = self.velkozz_con.get_index_comp_data("nyse")
            nasdaq_comp = self.velkozz_con.get_index_comp_data("nasdaq")
            self.logger.info(f"Queried Market composition data from the REST API. Queried {nyse_comp} NYSE, {nasdaq_comp} NASDAQ Stocks", "reddit_quant", "pipeline", 200) 

        except Exception as e:
            self.logger.error(f"Unable to query ticker data from the REST API. Exiting Pipeline w/ Error: {e}","reddit_quant", "pipeline", 400) 
            return

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
        ticker_freq_count = self._build_wsb_ticker_freq(wsb_posts, nyse_tickers, nasdaq_tickers)
        
        # Converting the data into a format expected by the REST API:
        formatted_freq_dicts = [
            {"day": date, "freq_counts":freq_count} 
            for date, freq_count in ticker_freq_count.items()
        ]  
        
        self.logger.info(f"Transformed {len(formatted_freq_dicts)} Tickers. Passing to the Loading method", "reddit_quant", "pipeline", 200) 

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
        try:
            response = requests.post(
            ticker_counts_endpoints,
            headers={"Authorization": f"Token {self.token}"},
            json=formatted_freq_dicts)

        except Exception as e:
            self.logger.error(f"HTTP Request to Velkozz Web API Failed, Request was not made due to Error: {e}", "reddit_quant", "pipeline", 400) 

        if response.status_code > 301:
            self.logger.error(f"POST request to Velkozz Web API failed. Status Code {response.status_code}. Response: {response.text}", "reddit_quant", "pipeline", 400) 
                
        else:
            self.logger.info(f"Sucessfully made POST request to Web API. Wrote {len(formatted_freq_dicts)} Ticker Freq Counts w/ Status Code: {response.status_code}", "reddit_quant", "pipeline", 200)

        # Wallstreet Bets Ticker Count Frequency Method:    
    
    def _build_wsb_ticker_freq(self, wsb_posts_df, *args):
        """Method converts structured timeseries data about wallstreetbets
        posts into a dict containing frequency counts of tickers mentioned
        in said posts.
        
        The method is designed to ingest data directly from the velkozz web
        api from the query api's '.get_subreddit_data()' method. It takes this
        dataframe and applys the following transformations to it:
        
        - Extracts frequency counts of all ticker mentions in the title and the
            body of all posts.
        - Resample all frequency counts by day.
        - Transform data into a dict in the form {"Timestamp":{dict of ticker frequency counts}}
        
        The method compares extracted ticker symbols from wsb posts that match with tickers
        found in lists of avaliable ticker symbols passed in as args. If none are provided
        the method will extract all 1-4 length capitalized strings, which will produce 
        unusable data.
        
        Args: 
            wsb_posts_df (pandas.DataFrame): The dataframe containing timeseries data 
                about r/wsb posts. 
                
            args (list): Method expects a list of ticker symbol lists. 
                
        Returns: 
            dict: The dictionary containing ticker frequency counts.
        """
        # Nested function to extract tickers from abody of text:
        def get_ticker(text):
        
            # Splitting the string into list of strings:
            words = [text.replace("$", "") for text in text.split()]    
            
            # Flattening the list args into single list:
            listed_tickers = list(itertools.chain(*args))
            #print(listed_tickers)
            
            # Extracting only length 1-4 captialized strings:
            tickers = set([
                word for word in words 
                if 1 < len(word) <= 4 
                and word.isupper() 
                and word in listed_tickers
            ])
                        
            return tickers 
        
        # Slicing dataframe to contain only relevant data:
        post_content_df = wsb_posts_df[["title", "content", "created_on"]]
        post_content_df.set_index("created_on", inplace=True)
        
        # Resetting Index to a datetime object:
        post_content_df.index = pd.to_datetime(post_content_df.index)
        
        # Creating columns containing unique ticker mentions:
        post_content_df["title_tickers"] = post_content_df["title"].apply(lambda x: get_ticker(x))
        post_content_df["content_tickers"] = post_content_df["content"].apply(lambda x: get_ticker(x))
        
        self.logger.info(f"Extracted {len(post_content_df['title_tickers'].values)} ticker symbols from the main wsb_post dataframe", "reddit_quant", "pipeline", 200) 

        # Splitting the dataframe into daily slices:
        data_dict = {}
        
        # Getting days listed by the dataframe:
        dayspan = post_content_df.index.floor("d").unique()
        self.logger.info(f"Extracted {len(dayspan)} days from the main dataframe timeseries", "reddit_quant", "pipeline", 200)

        for day in dayspan:
            date = datetime.strftime(day,'%Y-%m-%d')
            
            # Daily df slice:
            dayslice_df = post_content_df[date]
            
            # Extracing list of ticker variables:
            unique_title_tickers = [item for items in dayslice_df["title_tickers"].values for item in items]
            unqie_content_tickers = [item for items in dayslice_df["content_tickers"].values for item in items] 

            # Adding counters from title and content columns:
            ticker_collections = collections.Counter(unique_title_tickers) + collections.Counter(unqie_content_tickers)
            
            # Building the data_dict:
            data_dict[date] = ticker_collections
        
        self.logger.info(f"Finished compiling the ticker frequency dict. Returning a dict of {len(data_dict)} Ticker symbols and their frequency.", "reddit_quant", "pipeline", 200)

        return data_dict
