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
        self.finance_endpoint = f"{self.base_url}/finance_api"

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
        subreddit_endpoint = f"{self.reddit_endpoint}/r{subreddit_name}"

        # Making request to API:
        # Conditionals dealing with the start and end data params:
        if start_date is None and end_date is None:
            response = requests.get(subreddit_endpoint, headers=self.auth_header)
        
        #TODO: This is Horrific, fix this please. Add a switch like stop these conditionals oh god:
        else:
            if end_date is None:
                response = requests.get(
                    subreddit_endpoint, 
                    headers=self.auth_header,
                    params={"Start-Date":start_date}
                )

            if start_date is None:
                response = requests.get(
                    subreddit_endpoint, 
                    headers=self.auth_header,
                    params={"End-Date":end_date}
                )

            if start_date and end_date is not None:
                response = requests.get(
                    subreddit_endpoint, 
                    headers=self.auth_header,
                    params={"Start-Date":start_date,"End-Date":end_date}
                )
        
        # Converting the json response to formatted dataframe:
        if response.status_code < 302:
            raw_json = response.json()

            # Converting the JSON resposne to pandas dataframe:
            subreddit_df = pd.DataFrame.from_dict(raw_json, orient="columns")
            subreddit_df.set_index("id", inplace=True)
            subreddit_df.drop(["url"], axis=1, inplace=True)

            return subreddit_df

        else:
            raise ValueError(f"Request to subreddit {subreddit} api failed with status code {response.status_code}")

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
        if response.status_code < 302:
            raw_json = response.json()

            # Converting JSON data to pandas dataframe:
            index_comp_df = pd.DataFrame.from_dict(raw_json, orient='columns')
            index_comp_df.drop(['url'], axis=1, inplace=True)
            
            return index_comp_df
        
        else:
            pass

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

