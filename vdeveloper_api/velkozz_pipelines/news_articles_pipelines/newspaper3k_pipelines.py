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
import yaml

# Importing internal modules:
from vdeveloper_api.velkozz_pipelines.core_objects import Pipeline
from vdeveloper_api.velkozz_pipelines.utils import logger

# Importing Velkozz API:
from vdeveloper_api.velkozz_pywrapper.query_api.velkozz_api import VelkozzAPI

class NewsArticlesPipeline(Pipeline):
    """TODO: Add Documentation.
    """

    def __init__(self, **kwargs):

        # Initalizing parent pipeline method:
        super(NewsArticlesPipeline, self).__init__(**kwargs)


        self.config_file_path = kwargs.get("NEWS_SITE_CONFIG_PATH") if "NEWS_SITE_CONFIG_PATH" in kwargs else "pipeline_config.yaml"
        # Reading data from YAML config file:
        try:
            yaml_raw = open(self.config_file_path, "r")
            self.config_params = yaml.safe_load(yaml_raw)
        except:
            raise ValueError("Error Loading YAML Configuration File")

        # Extracting config params and using them to initalize Velkozz API:
        self.token = yaml.get("VELKOZZ_API_TOKEN")
        web_api_url = yaml.get("VELKOZZ_API_URL")

        # Creating Velkozz API Connection:
        self.query_con = VelkozzAPI(token=self.token, url=web_api_url)

        # TODO: Call execute graph function when code is written.

        pass 

    def extract_news_articles(self):
        pass

    def transform(self, *args):
        pass

    def load_listings_to_api(self, *args):
        pass

    def build_graph(self, **options):
        pass


