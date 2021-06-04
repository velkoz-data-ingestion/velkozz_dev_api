# Importing Data Manipulation packages:
import pandas as pd
import bonobo
import os
from datetime import date, timedelta, datetime
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
        self.token = self.config_params.get("VELKOZZ_API_KEY")
        web_api_url = self.config_params.get("VELKOZZ_API_URL")

        # Creating Velkozz API Connection & API endpoint:
        self.query_con = VelkozzAPI(token=self.token, url=web_api_url)
        self.velkozz_news_endpoint = f"{self.query_con.news_endpoint}/news_articles"

        self.execute_pipeline()

    def extract_news_sources(self):
        """Generator extracts the list of news website urls from the config file and
        uses newspaper3k to extract articles for each url.

        The method iterates through each provided news site url and uses the built
        in multi-threading to extract, and download articles. The array of now built
        sources is then iterated over and the generator passes each Souce on to the
        transformation method for article formatting and feature extraction
        
        Yields:
            tuple: A length 1 tuple containing a built newspaper Source() object. 

        """        
        # Extracting all of the news urls from the config file:
        news_urls = self.config_params["NewsSites"][1:4]
        
        # Building newspaper Source objects from each news url:
        built_news_source =[
            self.__create_source_obj(source_dict) for source_dict in news_urls]
        
        # Creating Pools for multi-threading download:
        news_pool.set(built_news_source, threads_per_source=2)
        news_pool.join()
        
        for source in built_news_source:
            yield source

    def transform_news_sources(self, *args):
        """The generator function ingests a single built Source object and unpacks 
        and parses each individual Article() object stored in the Source() object.
        
        Feature extraction is then performed on the article objects where key fields 
        as well as external meta-data are built into a python dict. This built python
        dict serves as the JSON object that is passed into the load method.
        
        Args:
            *args (tuple): A length 1 tuple containing a built Source() object.
            
        Yields:
            tuple: A length 1 tuple containing the JSON object of Article Dicts.
        """
        # Unpacking tuples:
        source = args[0]        
        source_name = args[0].organization
    
        articles = []    
        # Iterating through the list of articles building the list of features dict:
        for article in source.articles:
            try:
                # Parsing and NLP on articles:
                article.parse()
                article.nlp()

                article_features = {
                        "title": article.title,
                        "authors": article.authors,
                        "published_date": article.publish_date,
                        "article_text": article.text,
                        "meta_keywords": article.meta_keywords,
                        "nlp_keywords": article.keywords,
                        "url": article.url,
                        "source": source_name,
                        "timestamp": datetime.now()
                }
                articles.append(article_features)

            except:
                pass

        yield articles

    def load_listings_to_api(self, *args):
        """The method recieves the formatted JSON object from the
        transformation method, attaches it to a payload and makes 
        a POST request to the REST API.
        
        Args: 
            args (tuple): The length 1 tuple containing the formatted
                JSON object. 
        """
        # Unpacking tuple:
        articles_data = args[0]

        # Basic error checking of ingested arguemnts:
        if len(articles_data) < 1:
            logger.default_logger(f"Article Length:{len(articles_data)} Exiting w/o making a POST Request.")
            return
        
        # Request object and attaching payload:
        response = requests.post(
            self.velkozz_news_endpoint,
            headers={"Authorization":f"Token {self.token}"},
            json=articles_data)
        
        logger.default_logger(f"Made POST request to Velkoz Web API <News Articles: Length {len(articles_data)}> w/ Status Code: {response.status_code}")

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
            self.extract_news_sources,
            self.transform_news_sources,
            self.load_listings_to_api)

        return self.graph

    def __create_source_obj(self, news_source_dict):
        """The method ingests a key value pair containing base information
        about news sources and returns a fully configured and built newspaper
        Source() object with custom fields that are used to provide metadata
        for built Articles.
        
        Args: 
            news_source_dict (dict): The Key-Value pair of news source info in 
                the format {"Source Name": "Source Url"}
                
        Returns: 
            newspaper.Source: The fully built source from the input url.
        """
        # Converting dict and unpacking params:
        source_info = list(news_source_dict.items())[0]
        source_name = source_info[0]
        source_url = source_info[1]
        
        # Creating Custom Source object w/ additional params:
        newspaper = Source(source_url, memoize_articles=False)
        newspaper.organization = source_name 
        
        newspaper.build()
        
        return newspaper
 
test = NewsArticlesPipeline(
    NEWS_SITE_CONFIG_PATH="/Users/matthewteelucksingh/Projects/test_repos/velkozz_news_api/example_config.yaml"
)