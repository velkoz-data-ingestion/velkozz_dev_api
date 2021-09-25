# Importing Data Manipulation packages:
import pandas as pd
import bonobo
import os
from datetime import date, timedelta, datetime, time
import pytz
import requests
import bs4
from newspaper import Article, Source
from newspaper import news_pool
import time
import re
import yaml

# Importing internal modules:
from vdeveloper_api.velkozz_pipelines.core_objects import Pipeline
from vdeveloper_api.velkozz_pipelines.utils import logger

class CountrySummaryPipeline(Pipeline):
    """A Pipeline that writes summary information about all countries from 
    the public REST API service REST Countries to the Velkozz REST API.

    It performs ETL functions to format the REST Country data in a way that
    is ingestible by the REST API. This is a pipeline object that is designed to write
    'descrete/static' data. It is not expected to run frequently as the data that
    needs to be ingested is not expected to change rapidly.
    """
    def __init__(self, **kwargs):

        # Initalizing parent pipeline method:
        super(CountrySummaryPipeline, self).__init__(**kwargs)

        # Declaring the url to the REST API for Country data: 
        self.rest_countries_url = kwargs.get("REST_COUNTRY_URL") if "REST_COUNTRY_URL" in kwargs else "https://restcountries.com/v3/all"

        self.logger.info(f"Initalized REST_Countries Pipeline with Url: {self.rest_countries_url}" , "geography", "pipeline", 200)

        self.country_summary_endpoint = f"{self.web_api_url}/geography_api/countries/summary/"
        self.execute_pipeline()

    def extract_country_data_from_API(self):
        """The extract method makes a GET request to the REST Countries 
        external site and passes the response JSON package to the loading
        method.
        """
        # Making the GET request to the REST Country site:
        country_response = requests.get(self.rest_countries_url)
        
        if country_response.status_code < 301:
            self.logger.info(f"Made request to REST Countries and extracted {len(country_response.json())} w/ Status Code: {country_response.status_code}", "geography", "pipeline", 200)            
            yield country_response.json()
        else:
            self.logger.warning(f"REST Countries GET request returned status code {country_response.status_code}. Exiting Pipeline w/o writing data.", "geography", "pipeline", 301)
            return

    def load_country_data(self, *args):
        """Method makes POST request to the Velkozz REST API to write country summary data
        extracted via the extract_country_data_from_API method.
        """
        # Unpacking country data:
        countries = args[0]

        # Creating and making POST request to Velkozz API: 
        try:
            velkozz_response = requests.post(
                self.country_summary_endpoint,
                headers={"Authorization":f"Token {self.token}"},
                json=countries)
            self.logger.info(f"Wrote {len(countries)} countries to {self.country_summary_endpoint} w/ Status Code: {velkozz_response.status_code}. Exiting Pipeline",  "geography", "pipeline", 200)
        except Exception as e:
            self.logger.error(f"Error in making POST requests to the Velkozz API. Endpoint: {self.country_summary_endpoint}, Exited Pipeline w/o writing data w/ Error: {e}","geography", "pipeline", 400)

    def build_graph(self, **options):
        """The method that is used to construct a Bonobo ETL pipeline
        DAG that schedules the Pipelines ETL methods:

        Returns: 
            bonobo.Graph: The Bonobo Graph that is declared as an instance
                parameter and that will be executed by the self.execute_pipeline method.
        
        """
        # Building the Graph:
        self.graph = bonobo.Graph()    

        # Creating the main method chain for the graph:
        self.graph.add_chain(
            self.extract_country_data_from_API,
            self.load_country_data)

        return self.graph
