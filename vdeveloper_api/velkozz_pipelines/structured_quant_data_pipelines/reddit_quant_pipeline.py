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
import praw
from vdeveloper_api.velkozz_pywrapper.query_api.velkozz_api import VelkozzAPI
from vdeveloper_api.velkozz_pywrapper.api_utils.quant_data_utils.social_media_utils import build_wsb_ticker_freq

class WSBTickerFrequencyPipeline(Pipeline):
    """The object contains all of the methods usd to build a ETL pipeline
    for frequency of ticker mentions in wallstreetbets reddit posts.

    It inherits from the Pipeline API object with allows the extract, transform and load 
    methods to be overwritten but the graph creation and pipeline execution methods to be 
    inherited. The method queries wallstreetbets posts from the Velkozz Web API, extracts 
    unique elements, extracts the frequnecy counts for ticker mentions for each post.

    TODO: Finish Documentation.
    """
    pass