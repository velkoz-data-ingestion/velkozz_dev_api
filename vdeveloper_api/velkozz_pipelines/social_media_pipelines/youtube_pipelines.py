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
            CHANNEL_ID="UCmEu9Y8nodUV0jvsR9NYLJA",
            CHANNEL_NAME=" IWDominate",
            GOOGLE_API_KEY="AIzaSyCkRr8SFAlK2aHjPkBHY3hmISZ3jViRyBM",
            LOGGER_HOST="test",
            LOGGER_URL="test",
            token="97a6e8877fc4131bab181ff41283dd389048c6ef",
            VELKOZZ_API_URL="http://127.0.0.1:8000"
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
        try:
            self.youtube_api_obj = build("youtube", "v3", developerKey=self.google_api_key)
            self.logger.info(f"Google API service initialized as youtube v3 service: {self.youtube_api_obj}", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)
        
        except Exception as e:
            self.logger.error(f"Error with building Google-Youtube-API w/ Error: {e}. Exiting pipeline", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 400)
            return

        # Building Velkozz Channel Data Endpoint:
        self.youtube_endpoint = f"{self.web_api_url}/social_media_api/youtube/channel_daily/"

        self.logger.info(f"Executing Daily Youtube Channel Stats Pipeline", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)
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
        try:
            if self.channel_id is not None: # Make request for youtube channel based on ID over channel name.
                
                #  If the CHANNEL_ID is a lists, iterate through the lists and generate response objects:
                if isinstance(self.channel_id, list):
                    for id in self.channel_id:
                        response = self.youtube_api_obj.channels().list(
                            part="statistics",
                            id=self.channel_id)
                        self.logger.info(f"Built Youtube API response object with Channel ID {self.channel_id}. Response: {response}", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)

                        self.logger.info(f"Successfully Created Google-Youtube Channel Response Object: {response}", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)

                        response = response.execute()
                        self.logger.info(f"Executed response object, querying the Youtube-API",  "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)

                        yield response
                
                else:
                    response = self.youtube_api_obj.channels().list(
                        part="statistics",
                        id=self.channel_id)
                    self.logger.info(f"Built Youtube API response object with Channel ID {self.channel_id}. Response: {response}", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)
            
                    self.logger.info(f"Successfully Created Google-Youtube Channel Response Object: {response}", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)
                    
                    response = response.execute()
                    
                    self.logger.info(f"Executed response object, querying the Youtube-API",  "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)

                    yield response

            else:
                self.logger.error(f"No CHANNEL_ID found. Exiting the Pipeline w/o making a request to the Google API.",  "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 400)
                return                
            
        except Exception as e:
            self.logger.warning(f"Unable to build Google-Youtube Response object. Exited w/ Error {e}",  "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 400)
            return
   

    def transform_channel_stats(self, *args):
        """The method recieves the response dict from the extraction method and unpacks the
        key params from said response dict. 

        It re-builds the dict into a formatted request payload to be sent to the Velkozz REST API.
        The method constrcuts the following request payload:

        {
            "channel_id": "specific_channel_id",
            "channel_name": "Specific Channel name or None",
            "viewCount": 2124343,
            "subscriberCount": 3423523,
            "videoCount": 223
        }

        Yields:
            A length one list containing a dictionary as a fully formatted request payload.
        """
        # Unpacking the response tuple:
        response = args[0]
        channel_items = response["items"][0]
        channel_stats = channel_items["statistics"]

        # Extracting key variables:
        self.logger.info(f"Unpacking channel data from response object dict", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)
        channel_id = channel_items.get("id", None)
        viewCount = channel_stats.get("viewCount", None)
        subscriberCount = channel_stats.get("subscriberCount", None)
        videoCount = channel_stats.get("videoCount")

        # Building Payload Dict if all variables are correctly extracted from response dict:
        if None not in {channel_id, viewCount, subscriberCount, videoCount}: # Only if all values are not none.

            # Building request payload:
            self.logger.info(f"Building payload json dict. All data point sucessfully extracted and are not None.",  "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)
            payload = [{
                "channel_id":channel_id,
                "channel_name": self.channel_name,
                "viewCount": viewCount,
                "subscriberCount": subscriberCount,
                "videoCount": videoCount
            }]
        
        else: # Some of the values are none.
            self.logger.info(f"Building payload json dict. Not all data points have been found. Some are None. Channel ID: {channel_id}, Channel Name: {self.channel_name}, viewCounts: {viewCount}, subscriberCount: {subscriberCount}, videoCount: {videoCount}", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)
            payload = [{
                "channel_id":channel_id,
                "channel_name": self.channel_name,
                "viewCount": viewCount,
                "subscriberCount": subscriberCount,
                "videoCount": videoCount
            }]

        yield payload

    def load_channel_stats(self, *args):
        """The method recieves the built payload dict from the transformation method and
        writes the youtube chanel data to the Velkozz REST API.
        """
        # Unpacking Channel Payload data:
        channel_data_payload = args[0]

        if len(channel_data_payload) < 1:
            self.logger.warning(f"Only {len(posts_dict)} data points found. Existing w/o making a POST request to the API", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 301)
            return
        
        else:
            self.logger.info(f"Making POST request to the Web API to write daily channel data", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)
            
        # Making POST request to the API:
        try:
            response = requests.post(
                self.youtube_endpoint,
                headers={"Authorization":f"Token {self.token}"},
                json=channel_data_payload)
        
            if response.status_code > 300:
                self.logger.warning(f"POST Request of {len(channel_data_payload)} youtube channel data failed w/ Status Code {response.status_code}", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 301)
                
            else:
                self.logger.info(f"Made POST request to Youtube Channel Velkozz REST API with Status Code {response.status_code}", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 200)

        except Exception as e:
            self.logger.error(f"Error in Making POST Request to the API. Exited w/ Error: {e}", "youtube_daily", "pipeline", datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 400)

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
            self.load_channel_stats)
    
        return self.graph
