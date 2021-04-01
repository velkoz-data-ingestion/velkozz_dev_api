# Importing external packages:
import pandas as pd
import requests

class VelkozzAPI(object):
    """A python object representing a connection to the Velkozz Web API.  
    """

    def __init__(self, **kwargs):
        
        # Core Web API Configuration: TODO: Replace with config dict/env var.
        self.base_url = kwargs.get("url", "http://localhost:8000")
        # Creating url routes for core endpoints:
        self.token_endpoint = f"{self.base_url}/api-token-auth/" 
        self.reddit_endpoint = f"{self.base_url}/social_media_api/reddit"

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

                    

