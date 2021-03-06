# Importing external packages:
import pandas as pd
import requests
import collections
import json
import ast
import itertools

class VelkozzAPI(object):
    """A python object representing a connection to the Velkozz Web API.  
    """
    def __init__(self, **kwargs):
        
        # Core Web API Configuration: TODO: Replace with config dict/env var.
        self.base_url = kwargs.get("url", "http://localhost:8000")
        # Creating url routes for core endpoints:
        self.token_endpoint = f"{self.base_url}/api-token-auth/" 
        self.reddit_endpoint = f"{self.base_url}/social_media_api/reddit"
        self.jobs_endpoint = f"{self.base_url}/social_media_api/jobs"
        self.youtube_endpoint = f"{self.base_url}/social_media_api/youtube"
        self.finance_endpoint = f"{self.base_url}/finance_api"
        self.news_endpoint = f"{self.base_url}/news_api"

        # Extracting necessary params from kwargs: 
        self.username = kwargs.get("username", None)
        self.password = kwargs.get("password", None)
        
        # If token not provided calls the token retrieval function:
        self.token = kwargs['token'] if "token" in kwargs else self._get_user_token()
        
        # Building base authentication HTPP header:
        self.auth_header = {
            "Content-Type": "application/json",
            "Authorization":f"Token {self.token}"}

        # TODO: Once API that return remaining requests/access status is written include that.

    # Social Media Query Methods:
    def get_subreddit_data(self, subreddit_name, start_date=None, end_date=None):
        """Method queries the velkozz api for posts in a given subreddit according
        to the specified start and end dates. 

        The method formats the API response into a pandas dataframe.

        Args:
            subreddit_name (str): The name of the subreddit that the data will be
                queried from.

            start_date (str|None, optional): The day that will serve as the start of
                the dataset. 

            end_date (str|None, optional):  The day that will serve as the end of the
                dataset.
        
        Returns:
            pd.DataFrame: The dataframe containing all of the formatted subreddit data.

        """
        # Building subreddit enpoint:
        reddit_top_posts_endpoint = f"{self.reddit_endpoint}/top_posts"

        # Making request to API:
        params = {}
        
        # Conditionals dealing with the start and end data params:
        if start_date is not None:
            params["Start-Date"] = start_date

        if end_date is not None:
            params["End-Date"] = end_date

        # Conditionals filtering query based on subreddit:
        if subreddit_name is not None:
            params["Subreddit"] = subreddit_name

        # Making the query to the API:
        response = requests.get(
            reddit_top_posts_endpoint,
            headers=self.auth_header,
            params=params
        )

        # Converting the json response to formatted dataframe:
        try:
            if response.status_code <= 302:
                raw_json = response.json()

                # Converting the JSON resposne to pandas dataframe:
                subreddit_df = pd.DataFrame.from_dict(raw_json, orient="columns")
                subreddit_df.set_index("id", inplace=True)
                subreddit_df.drop(["url"], axis=1, inplace=True)

                return subreddit_df

            else:
                raise ValueError(f"Request to subreddit {subreddit} api failed with status code {response.status_code}")
        
        except Exception as e:
            return f"Error w/ Constructing Dataframe with Error: {e}" 
    
    def get_indeed_job_listings(self, job_type=None, location=None, company=None, start_date=None, end_date=None):
        """The method queries the velkozz api for all of the job postings from 
            the indeed job posts database. 
            
            It transforms the JSON response from the REST API into a formatted
            pandas dataframe.

            start_date (str|None, optional): The day that will serve as the start of
                the dataset. 

            end_date (str|None, optional):  The day that will serve as the end of the
                dataset.

            location (str|None, optional): The location of the job listings that are 
                queried from the api.

            job_type (str|None, optional): The search string that dictates the type of job
                listings that are queried from the api.

            company (str|None, optional): The search string that filter the companies being
                queried from the api.
        
            Returns:
                pd.DataFrame: The dataframe containing all of the formatted indeed listings data.

        """
        # Building indeed.com endpoint:
        indeed_jobs_endpoint = f"{self.jobs_endpoint}/indeed/listings/"

        # Conditionals dealing with the start and end data params:
        if start_date is None and end_date is None:
            payload = {}

        # TODO: Replace with new python switches?        
        else:
            if end_date is None:
                payload = {"Start-Date":start_date}

            if start_date is None:
                payload = {"End-Date":end_date}

            if start_date and end_date is not None:
                payload = {"Start-Date":start_date,"End-Date":end_date}

        # Conditionals searching for job listings types:
        if job_type is not None:
            payload["job"] = job_type

        # Conditionals for locations:
        if location is not None:
            payload["location"] = location

        # Conditionals for company:
        if company is not None:
            payload["company"] = company

        # Making GET request to the velkozz api once the query params have been built:
        response = requests.get(indeed_jobs_endpoint, headers=self.auth_header, params=payload)

        # Extracting JSON response and converting to pandas dataframe:
        try:
            if response.status_code <= 302:
                raw_json = response.json()

                # JSON -> DataFrame:
                indeed_jobs_df = pd.DataFrame.from_dict(raw_json, orient="columns")
                indeed_jobs_df.set_index("id", inplace=True)
                indeed_jobs_df.drop(["url"], axis=1, inplace=True)

                return indeed_jobs_df
            
            else:
                return (f"Error with Response Object: {response}", response)

        except Exception as e:
            return f"Error w/ Constructing Dataframe with Error: {e}"

    def get_daily_youtube_channel_stats(self, start_date=None, end_date=None, channel_name=None, channel_id=None):
        """The method that queries the Velkozz REST API for data about Youtube Channel Daily Statistics.

        The method builds the specific API endpoint for daily youtube channel statistics and makes a POST
        request to the API. It filters the API query based on the params passed into the method. The JSON
        that is extracted from the API endpoint is converted to a structured pandas dataframe.

        Args:

            start_date (str|None, optional): The day that will serve as the start of
                the dataset. 

            end_date (str|None, optional): The day that will serve as the end of the
                dataset.

            channel_name (str|None, optional): The name of the youtube channel that will
                be used to fliter the dataset (only data for this specific channel will be queried).

            channel_id (str|None, optional): The google specific ID of the youtube channel that
                will be used to filter the dataset (only data for this specific channel will be queried).
            
        Returns:
            pd.Dataframe: The dataframe containing all formatted youtube channel datapoints.

        """
        # Building the specific daily youtube channel endpoint:
        daily_youtube_channel_endpoint = f"{self.youtube_endpoint}/channel_daily/"

        # Conditionals dealing with the start and end data params:
        if start_date is None and end_date is None:
            payload = {}

        # TODO: Replace with new python switches?        
        else:
            if end_date is None:
                payload = {"Start-Date":start_date}

            if start_date is None:
                payload = {"End-Date":end_date}

            if start_date and end_date is not None:
                payload = {"Start-Date":start_date,"End-Date":end_date}

        # Filtering based on channel ID specifics:
        if channel_name is not None:
            payload["Channel-Name"] = channel_name

        if channel_id is not None:
            payload["Channel-ID"] = channel_id

        # Making GET request to the velkozz api once the query params have been built:
        response = requests.get(daily_youtube_channel_endpoint, headers=self.auth_header, params=payload)
        
        # Extracting the response body from the API request:
        try:
            if response.status_code <= 302:
                raw_json = response.json()

                # JSON -> DataFrame:
                youtube_channel_df = pd.DataFrame.from_dict(raw_json, orient="columns")
                youtube_channel_df.set_index("date_extracted", inplace=True)
                youtube_channel_df.drop(["url"], axis=1, inplace=True)

                return youtube_channel_df
            
            else:
                return (f"Error with Response Object: {response}", response.json)

        except Exception as e:
            return f"Error w/ Constructing Dataframe with Error: {e}"

    # Finance Data Query Methods:
    def get_index_comp_data(self, market_index):
        """Method queries the velkozz api for market index composition.

        The method formats the API JSON response into a pandas dataframe.

        Args:
            market_index (str): The shortened name of the market being queried.
                eg: spy, djia, nyse.

        Returns:
            pd.DataFrame: The dataframe containing all of the formatted market index
                composition.

        """
        # Building api endpoint:
        market_index_endpoint = f"{self.finance_endpoint}/market_index/{market_index}comp"
        
        # Making request to the API JSON data:
        response = requests.get(market_index_endpoint, headers=self.auth_header)

        # Extracting response content in JSON format:
        try:
            if response.status_code < 302:
                raw_json = response.json()

                # Converting JSON data to pandas dataframe:
                index_comp_df = pd.DataFrame.from_dict(raw_json, orient='columns')
                index_comp_df.drop(['url'], axis=1, inplace=True)
                
                return index_comp_df
            
            else:
                return (f"Error with Response Object: {response}", response)

        except Exception as e:
            return f"Error w/ Constructing Dataframe with Error: {e}"

    # Strucutred Finance Quant Data Query Methods:
    def get_wsb_ticker_counts(self, start_date=None, end_date=None):
        """Method queries the velkozz api for the frequency counts of ticker mentions
        from the wallstreetbets subreddit.

        It performs a GET request to receive the frequency count data from the REST API and
        converts the JSON data to a structured timeseries dataframe.

        JSON data format: {
            "day": "2021-03-23",
            "ticker_count": "{'FUBO': 1, 'AM': 1, 'UWMC': 1, 'FOR': 2, 'TX': 1, 'GME': 31}
            }

        Formatted DataFrame Format:

        +------------------+--------+------+------+-------+-----+-----+
        |       Date       |  FUBO  |  AM  | UWMC |  FOR  | TX  | GME |
        +------------------+--------+------+------+-------+-----+-----+
        | Datetime (index) |   int  |  int |  int |  int  | int | int |
        +------------------+--------+------+------+-------+-----+-----+

        Args:
            start_date (str|None, optional): The day that will serve as the start of
                the dataset. 

            end_date (str|None, optional):  The day that will serve as the end of the
                dataset.

        Returns:
            pd.DataFrame: The dataframe containing all of the ticker frequency counts.

        """
        # Building api endpoints:
        wsb_ticker_counts_endpoint = f"{self.finance_endpoint}/structured_quant/wsb_ticker_mentions"

        # Making the request to the REST API:
        response = requests.get(wsb_ticker_counts_endpoint, headers=self.auth_header)

        # Extracting response content in JSON format:
        if response.status_code < 302:
            raw_json = response.json()

            # Create a list of all unique ticker symbols present in dataset:
            unique_tickers_lst = []
            date_index = []

            for ticker_freq_dict in raw_json:
                unique_tickers_lst.append(ast.literal_eval(ticker_freq_dict["ticker_count"]).keys())
                date_index.append(ticker_freq_dict['day'])

            # Flattening list of dict keys into a list of ticker symbols:
            unique_tickers_lst = set(itertools.chain(*unique_tickers_lst))

            # Building the ticker mentions DataFrame:
            wsb_ticker_freq_df = pd.DataFrame(index=date_index, columns=unique_tickers_lst)

            # Populating dataframe with data O(n^3):
            for frequency_dict in raw_json:
                for key, value in ast.literal_eval(frequency_dict["ticker_count"]).items():
                    wsb_ticker_freq_df.loc[frequency_dict["day"]][key] = value

            wsb_ticker_freq_df.fillna(0, inplace=True)

            return wsb_ticker_freq_df

    # News Articles Query Methods 
    def get_news_articles(self, start_date=None, end_date=None, source=None):
        """The method queries the REST API for newspaper articles. 

        It performs a GET request to the news_api/news_articles endpoint with
        search params. The JSON data that the API returns is transformed into
        a pandas dataframe and returned.

        The dataframe that is built is in the following format:

        +----------------------------------------------------------------------------------------------------------+
        | title | published  | authors | content | meta_keywords | nlp_keywords | article_url | source | timestamp |
        +-------+------------+---------+---------+---------------+--------------+-------------+--------+-----------+
        |  str  |  datetime  |   list  |   srt   |      list     |     list     |     str     |   str  |  datetime |
        +-------+------------+---------+---------+---------------+--------------+-------------+--------+-----------+

        Args:
            start_date (str|None, optional): The day that will serve as the start of
                the dataset. 

            end_date (str|None, optional):  The day that will serve as the end of the
                dataset.

            source (str|None, optional): The name of the source of articles being queried from the database.
                Example: "CNN". 

        Return:
            pd.DataFrame: The formatted dataframe containing news articles.
        """
        # Building the news articles API endpoint:
        news_article_endpoints = f"{self.news_endpoint}/news_articles"

        # Conditionals dealing with the start and end data params:
        if start_date is None and end_date is None:
            payload = {}
        
        else:
            if end_date is None:
                payload = {"Start-Date":start_date}

            if start_date is None:
                payload = {"End-Date":end_date}

            if start_date and end_date is not None:
                payload = {"Start-Date":start_date,"End-Date":end_date}

        if source is not None:
            payload["Source"] = source

        # Creating the GET request to the API:
        response = requests.get(news_article_endpoints, headers=self.auth_header, params=payload)

        # Extracting the JSON data from the response object if GET request was sucessful:
        try:
            if response.status_code <= 302:
                raw_json = response.json()
                
                # JSON -> DataFrame:
                news_articles_df = pd.DataFrame().from_dict(raw_json, orient="columns")
                news_articles_df.set_index("title", inplace=True)

                return news_articles_df
            else:
                return (f"Error with Response Object: {response}", response)
        
        except Exception as e:
            return f"Error w/ Constructing Dataframe with Error: {e}"

    #  <-- Internal assistance methods -- >
    def _get_user_token(self):
        """Method makes a POST request to the velkozz authentication
        endpoint to extract an auth token for the user if the user
        is given a username and password keyword

        Returns:
            str | None: The fully formatted auth token as a string 
                or a None type if the request fails.
        """
        if self.username is not None and self.password is not None:
            token_response = requests.post(
                self.token_endpoint,
                data = {
                    "username": self.username,
                    "password": self.password
                })
        
            return token_response.json()["token"] 

        else:
            raise ValueError("No Account auth provided to interact with web api. Check config params") 
