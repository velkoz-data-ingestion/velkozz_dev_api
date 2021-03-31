# Importing Data Manipulation packages:
import pandas as pd
import bonobo
import os
from datetime import date, timedelta, datetime
import pytz
import requests

# Python API Wrappers:
import praw
from query_api.velkozz_api import VelkozzAPI    

# Importing internal modules:
from core_objects import Pipeline
from utils import logger
    
class RedditContentPipeline(Pipeline):
    """An object that contains all the logic and methods necessary to construct a 
    ETL pipeline for extracting and ingesting daily relevant subreddit posts to a database.
    
    It inherits from the Pipeline API object with allows the extract, transform and load 
    methods to be overwritten but the graph creation and pipeline execution methods to be 
    inherited. The object extracts filings from the "Top" and "Rising" tabs of a subreddit.
    Each of these Tab's context is extracted by a sperate Extraction method which
    are then both fed into a transformation method which normalizes the data into 
    a standard format to be written to the database. 
    
    See graphviz plots of the bonobo graph for a structure outline of how data flows.
    Once again all credit goes to Bonobo and Pandas for the actual heavy lifting.

    Example:
        test_pipeline = EDGARFilingsPipeline("test.sqlite", "learnpython")
 
     Arguments:
        dbpath (str): The relative or absoloute database URL pointing to
            the database where stock price data should be written.
        subreddit (str): The string that indicates the specific subreddit
            that the data is to be scraped from.
    """
    def __init__(self, subreddit_name, **kwargs):

        # Initalizing the parent Pipeline object:
        super(RedditContentPipeline, self).__init__(**kwargs)
   
        # Reddit pipeline configuration objects:
        self.subreddit_name = subreddit_name

        self.token = kwargs.get("token")

        # Attempting to extract praw config from kwargs: 
        # Praw configs extract from kwargs then search env variables if not found:
        client_id = kwargs["CLIENT_ID"] if "CLIENT_ID" in kwargs else os.environ["CLIENT_ID"]
        client_secret = kwargs["CLIENT_SECRET"] if "CLIENT_SECRET" in kwargs else os.environ["CLIENT_SECRET"]
        user_agent = kwargs["USER_AGENT"] if "USER_AGENT" in kwargs else  os.environ["USER_AGENT"]

        # Creating connection to the Velkozz Web API via Query API wrapper:
        self.query_con = VelkozzAPI(token=self.token)

        # Creating a reddit praw instance based on specified subreddit:
        self.reddit = praw.Reddit(
            client_id = client_id,
            client_secret= client_secret,
            user_agent = user_agent
        )

        self.subreddit = self.reddit.subreddit(self.subreddit_name)

        logger.default_logger(f"Reddit Instance Initalized with Read Status:{self.reddit.read_only}")

        # Execuring all of the ETL functions mapped in the graph:
        self.execute_pipeline()

    def extract_daily_top_posts(self):
        """Method extracts the daily top reddit submissions from a subreddit
        via the praw API wrapper.

        The generator yields a dictionary containing relevant information extracted from each post generated
        from the subreddit.top(day) praw method. All top posts are compiled into a nested dict that is then 
        passed into a data transformation method. The data is compiled into a dict for speed as it is then 
        converted into a dataframe in the data transformation method. All seaching and transformation of raw 
        data is done prior to it being converted to a dataframe.

        Yields: Dict
            A dictionary containing all the relevant information for each reddit post
                necessary to compile a dataframe:
                {
                    id1: {title, content, upvote_ratio, score, num_comments, created_on, stickied, over_18, spoiler, permalink, author},
                    id2: {title, content, upvote_ratio, score, num_comments, created_on, stickied, over_18, spoiler, permalink, author},
                                                ...
                    idn: {title, content, upvote_ratio, score, num_comments, created_on, stickied, over_18, spoiler, permalink ,author},
                }
        """
        posts_dict = {}

        # Iterating through the rising posts constructing and generating dicts:
        for post in self.subreddit.top("day"):

            # Building the single dict key-value pair:
            post_content_dict = {
                "title": post.title,
                "content":post.selftext,
                "upvote_ratio":post.upvote_ratio,
                "score":post.score,
                "num_comments":post.num_comments,
                "created_on": self._format_datetime(post.created_utc), 
                "stickied":post.stickied,
                "over_18":post.over_18,
                "spoiler":post.spoiler,
                "link":post.permalink,
                "author":post.author
            }

            posts_dict[post.id] = post_content_dict

        yield posts_dict

    def transform_posts(self, *args):
        """The method recieves a length 1 tuple containing a dict of reddit posts generated from
        the extraction methods and performs transformation on the dict to convert it into a list of
        unqiue post dictionaries in the format:
        
        [
             {id1, title, content, upvote_ratio, score, num_comments, created_on, stickied, over_18, spoiler, permalink, author},
             {idn,title, content, upvote_ratio, score, num_comments, created_on, stickied, over_18, spoiler, permalink, author}
        ]
                    
        The transformation method queries the main web api for existing posts posted on the day that the pipeline
        is executed using the python query api. It compares the index of the database data and the index of the 
        data recieved from the extraction methods. Only unique elements not already in the database are passed 
        into the load method.

        When converting a dictionary to a unique elements Dataframe the method unpacks
        the information and converts all data to the correct data types. 
                
        Yields:
            List: A list of dictionaires where each dict is a unique subreddit post.

        """
        # Unpacking Args Tuple:
        posts_dict = args[0]
        
        # Querying existing posts from the database during current day:
        start_date = date.today() - timedelta(days=1) # Today's date range for API.
        end_date = start_date + timedelta(days=2)

        existing_posts_id = []
        try:
            existing_posts = self.query_con.get_subreddit_data(
                self.subreddit_name, 
                start_date=start_date, 
                end_date=end_date)
                
            existing_posts_id = existing_posts.index
        except:
            pass

        # Extracting unqiue keys from the posts_dict.keys() that are not present in the existing_post_id:
        unique_id_keys = list(set(posts_dict.keys()) - set(existing_posts_id))
        
        # Unpacking the "Author" parameter and extending Author derived params to the end of the content
        #  list for each dict key-value pair that is unique (not in the database):

        # Creating the list of unique post dicts to be passed to the load function:
        unique_posts = [

            # Unpacking list for faster appending:
            self._transform_post_content_lst(post_id, content_lst) for post_id, content_lst 
            in posts_dict.items() if post_id in unique_id_keys
            
        ]

        yield unique_posts

    def load_posts(self, *args):

        """Method writes the transfomred data to the Velkozz Web API. 

        Formatted data recieved from the transform method is recieved in the format:

        [
            {post1},
            {post2},
              ...
            {postn}
        ]

        This list of dicts is then packaged as a json object, attaced to a POST request and
        sent to the Web API.

        Arguments:
            args (tuple): The arguments passed into the load method by the transform method
                containing the dataframe. 
        
        """
        posts_dict = args[0]

        # If the list of posts contains no entries end w/o making POST request:
        if len(posts_dict) < 1:
            logger.default_logger(f"{len(posts_dict)} Unique posts found for daily top {self.subreddit_name}. Exiting w/o making POST request.")
            return

        # Building the API endpoint for the specific subreddit:
        subreddit_endpoint = f"{self.query_con.reddit_endpoint}/r{self.subreddit_name}/"

        # Making the post Request:
        response = requests.post(
            subreddit_endpoint, 
            headers={"Authorization":f"Token {self.token}"},
            json=posts_dict)

        logger.default_logger(f"Made POST request to Velkoz Web API <{subreddit_endpoint}> w/ Status Code: {response.status_code}")

    def build_graph(self, **options):
        """The method that is used to construct a Bonobo ETL pipeline
        DAG that schedules the following ETL methods:
        - Extraction: extract_daily_top_posts, extract_rising_posts
        - Transformation: transform_posts
        - Loading: load_posts
        Returns: 
            bonobo.Graph: The Bonobo Graph that is declared as an instance
                parameter and that will be executed by the self.execute_pipeline method.
        
        """
        # Building the Graph:
        self.graph = bonobo.Graph()    

        # Creating the main method chain for the graph:
        self.graph.add_chain(
            self.extract_daily_top_posts,
            self.transform_posts,
            self.load_posts)
            

        return self.graph

    def _transform_post_content_lst(self, post_id, post_dict):
        """The method that ingests a post id and dictionary of post content and 
        combines them into a post dictionary of full post content.

        It does this by adding the "id" param to the post_dict and by unpacking the 
        "author" param in the post_dict into variables that represent:

        - gold status
        - mod status
        - verified email status
        - the day the account was created
        - the comment karma of the account
        - the raw account name instead of a Redditor object.

        These params are unpacked into another dict which is then appended to the main post_dict.

        This method was initally performed by dict comprehension within the main transformation method but 
        was moved into an internal callable method to add error catching logic. This method is still 
        called within the main list comprehension:
        
        Arguments:
            post_id (str): The id string that is extracted from the unique elements dict that is added to 
                the new dict.  

            post_dict (dict): A dict contaiing all the extracted reddit data in key value pairs

        Returns:
            Dict: The transformed dict with full feature extraction and error-catching as described 
                above.

        """                
        # TODO: For Gods sake this is the laziest error-catching I have ever written please make this less horrible:
        
        try:            
            # Post dict autor dicts:
            author_dicts = {
                "id": post_id,
                "author_gold": post_dict["author"].is_gold,
                "mod_status": post_dict["author"].is_mod,
                "verified_email_status": post_dict["author"].has_verified_email,
                "acc_created_on": self._format_datetime(post_dict["author"].created_utc),
                "comment_karma": post_dict["author"].comment_karma,
                "author": post_dict["author"].name
            }

        except:
            # Post dict autor dicts:
            author_dicts = {
                "id": post_id,
                "author_gold": None,
                "mod_status": None,
                "verified_email_status": None,
                "acc_created_on": None,
                "comment_karma": None,
                "author": None
            }


        # Updating the main post_dict with the new author_dict content:
        post_dict.update(author_dicts)
        
        return post_dict    

    def _format_datetime(self, utc_float):
        """Method ingests a utc timestamp float and formats it into
        a datetime format that is prefered by the DRF framework.

        It performs conversions through datetime and pytz.

        Args:
            utc (float): A unix timestamp.   

        Returns:
            str: The formatted datetime in string format.
        """
        date_obj = datetime.fromtimestamp(utc_float, tz=pytz.utc)
        date_str = date_obj.strftime("%Y-%m-%dT%H:%M:%S.%f%z")

        return date_str
        
