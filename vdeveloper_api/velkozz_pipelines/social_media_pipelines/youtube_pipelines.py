# Importing Data Manipulation packages:
import pandas as pd
import bonobo
import os
from datetime import date, timedelta, datetime
import pytz
import requests
import bs4
import time
import re

# Importing Youtube API packages:
from googleapiclient.discovery import build

# Importing internal modules:
from vdeveloper_api.velkozz_pipelines.core_objects import Pipeline
from vdeveloper_api.velkozz_pipelines.utils import logger

class DailyYoutubeChannelStatsPipeline(Pipeline):
    """The pipeline object that contains all the logic to construct an ETL pipeline
    for extrating and ingesting daily youtube channel statistics.

    The pipeline makes use of the Google Youtube API to extract basic channel statistics
    based on the channel name or channel ID. The daily channel statistics are then 
    extracted from the Youtube API response and written to the Velkozz API.

    Example:
        test_pipeline = DailyYoutubeChannelStatsPipeline(
            token="test",
            VELKOZZ_API_URL="test_url",
            channel_id="example_id"
        )

    """
    def __init__(self, **kwargs):

        # Initalizing the parent Pipeline object:
        super(DailyYoutubeChannelStatsPipeline, self).__init__(**kwargs)

        # Extracting channel configuration params: 
        self.channel_id = kwargs.get("CHANNEL_ID", None)         
        self.channel_name = kwargs.get("CHANNEL_NAME", None)

        # Extracting Google-Youtube Developer API:
        self.google_api_key = kwargs.get("GOOGLE_API_KEY", None)

        # Building the Google API service:
        self.youtube_api_obj = build("youtube", "v3", developerKey=self.google_api_key)

        # Building Velkozz Channel Data Endpoint:
        self.youtube_endpoint = f"{self.web_api_url}/social_media_api/youtube/channel_daily"

        self.execute_pipeline()

    def extract_channel_stats(self):
        """The method uses the Google-Youtube-API to query daily channel statistics for the specific
        youtube channel given by the channel ID or the channel name. 

        This data is passed on in its raw format to the transform method where specific channel stats 
        are extracted.

        Yields:
            Dict: The dict containing the raw response data extracted from the Google-Youtube-API.

        """
        # Making the response to the Google API:
        if self.channel_id is not None: # Make request for youtube channel based on ID over channel name.
            response = self.youtube_api_obj.channels().list(
                part="statistics",
                id=self.channel_id)
        else: # Using the youtube channel name to make query of channel_id is none.
            response = self.youtube_api_obj.channels().list(
                part="statistics",
                forUsername=self.channel_name)
        
        response.execute()

        print(response.status)

    def transform_channel_stats(self):
        pass
    
    def load_channel_stats(self):
        pass

    def build_graph(self, **options):
        """The method that is used to construct a Bonobo ETL pipeline
        DAG that schedules the following ETL methods:

        - Extraction/Transformation: extract_channel_stats
        - Transform: transform_channel_stats
        - Loading: load_channel_stats

        Returns: 
            bonobo.Graph: The Bonobo Graph that is declared as an instance
                parameter and that will be executed by the self.execute_pipeline method.
        
        """
        # Building the Graph:
        self.graph = bonobo.Graph()    

        # Creating the main method chain for the graph:
        self.graph.add_chain(
            self.extract_channel_stats,
            self.transform_channel_stats,
            self.transform_channel_stats)
    
        return self.graph


DailyYoutubeChannelStatsPipeline(
    CHANNEL_ID="UCnQC_G5Xsjhp9fEJKuIcrSw",
    VELKOZZ_API_URL="http://127.0.0.1:8000",
    token="97a6e8877fc4131bab181ff41283dd389048c6ef",
    GOOGLE_API_KEY="AIzaSyCkRr8SFAlK2aHjPkBHY3hmISZ3jViRyBM"
    )