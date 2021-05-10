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

# Importing internal modules:
from vdeveloper_api.velkozz_pipelines.core_objects import Pipeline
from vdeveloper_api.velkozz_pipelines.utils import logger

# Python API Wrappers:
import praw
from vdeveloper_api.velkozz_pywrapper.query_api.velkozz_api import VelkozzAPI

class IndeedJobListingsPipeline(Pipeline):
    """The pipeline object that contains all the logic to construct an ETL pipeline
    for extrating and ingesting job postings from indeed.com.

    The pipeline recursivley scrapes the indeed website for all job listings given a 
    job title, location and maximum number of pages. Once it extracts the html content
    from the pages it parses the html for each individual job listing and extracts the
    key information. This information is then seralized and posted to the REST API.

    Example:
        test_pipeline = IndeedJobListingsPipeline("software developer", "Ontario", 10)

    """
    def __init__(self, job_type, search_area, max_pages, **kwargs):

        # Initalizing the parent Pipeline object:
        super(IndeedJobListingsPipeline, self).__init__(**kwargs)
   
        # Indeed pipeline configuration objects:
        self.job_type = job_type
        self.search_area = search_area
        self.max_pages = max_pages

        self.token = kwargs.get("token")

        # Base Indeed URL (canadian):
        self.base_indeed_url = "https://ca.indeed.com"

        # Velkozz Web API Configs:
        web_api_url = kwargs["VELKOZZ_API_URL"] if "VELKOZZ_API_URL" in kwargs else os.environ['VELKOZZ_API_URL']

        # Creating connection to the Velkozz Web API via Query API wrapper:
        self.query_con = VelkozzAPI(token=self.token, url=web_api_url)
        # API Endpoint for Indeed Job Postings:
        self.velkozz_indeed_endpoint = f"{self.query_con.jobs_endpoint}/indeed/listings/"

        # Execuring all of the ETL functions mapped in the graph:
        self.execute_pipeline()


    def recursively_extract_and_transform_listings(self):
        """
        - Inital Indeed URL, built based on search params.
        - Calls recursive method to extract job listings.
        - Convert to json and write to REST API.
        """
        # Building indeed url based on search params:
        built_indeed_url = f"{self.base_indeed_url}/jobs"

        # Building the URL with params via Prepared Requests:
        payload = {"q":self.job_type, "l":self.search_area}
        url_req = requests.Request("GET", built_indeed_url, params=payload).prepare()

        # Using recursive method to extract data via built url:
        job_listings = self._extract_indeed_jobs(url_req.url, 0, self.max_pages)
        
        logger.default_logger(f"Extracted {len(job_listings)} from Indeed.ca, scraping {self.max_pages} Pages")

        yield job_listings
        
    def load_listings_to_api(self, *args):
        """
        Loading data as JSON and making POST request to REST API.
        """
        # Unpackinig tuple:
        job_listings = args[0]

        # If the list of posts contains no entries end w/o making POST request:
        if len(job_listings) < 1:
            logger.default_logger(f"{len(job_listings)} No Data Recieved from recursive extraction method. Exiting w/o making POST request.")
            return

        # Seralizing the lit of dicts to json format:
        response = requests.post(
            self.velkozz_indeed_endpoint,
            headers={"Authorization":f"Token {self.token}"},
            json=job_listings)

        logger.default_logger(f"Made POST request to Velkoz Web API <Indeed Jobs Listings: Length {len(job_listings)}> w/ Status Code: {response.status_code}")

    def build_graph(self, **options):
        """The method that is used to construct a Bonobo ETL pipeline
        DAG that schedules the following ETL methods:

        - Extraction/Transformation: recursively_extract_and_transform_listings
        - Loading: load_listings_to_api

        Returns: 
            bonobo.Graph: The Bonobo Graph that is declared as an instance
                parameter and that will be executed by the self.execute_pipeline method.
        
        """
        # Building the Graph:
        self.graph = bonobo.Graph()    

        # Creating the main method chain for the graph:
        self.graph.add_chain(
            self.recursively_extract_and_transform_listings,
            self.load_listings_to_api)
            
        return self.graph

    def _get_post_date(self, date_str):
        """The method takes the text extracted from indeed.com's <span: 'date'>
        and performs datetime calculations to determine the date that the
        job listing was posted.
        
        Eg:
            If the current date is 07/04/21 then the function is applied as follows: 
            get_post_date("6 days ago") = 01/04/21.
            
        Args:
            date_str (str): The string of text that represents the string extracted
                from indeed.com's job post listings indicating how long ago a job 
                was posted.

        Returns:
            datetime: The absoloute date that the job listing was posted in the format
                dd/mm/yyyy
                
        """
        # Determining current day:
        today = datetime.today()

        # Attempting to extract number of days referenced:
        if re.search('\d+', date_str) and "day" in date_str:
            date_str = date_str.replace("+", "")
            
            # Extracting numbers from date string:
            day_num = [int(s) for s in date_str.split() if s.isdigit()][0]        
            
            # Performing calculation to determine post day:
            post_date = today - timedelta(days=day_num)
            
            return post_date.strftime('%Y-%m-%d')
        
        else:
            # Current day posted strings list:
            current_day_strs = ["Just posted", "Today"]
        
            # Add logic for unqiue Date values ("just now, today, etc").
            if date_str in current_day_strs:
                return today.strftime('%Y-%m-%d')

    def _get_listings_and_next_link(self, indeed_url):
        """Method uses requests and beautifulsoup to extract all relevant 
        job listings information from an indeed page as well as the link 
        to the "next" job listings page.

        Args:
            indeed_url (str): The url for the indeed page that is being
                scraped.
                
        Returns:
            list: A two element list containing [a list of dicts w/ each dict being a job listing,
                the full url to the next indeed listings page].
        """
        # Nested function for extracting company name from html object:
        def get_company_name(listing_card_element):
            """Method contains the logic for try-catching html extraction
            exceptions to be used with inline list comprehension.
            """
            try:
                company = listing_card_element.find("span", {"class":"company"}).text.strip('\n')
            except:
                company = None
            return company
        
        def get_summary(listing_card_element):
            """Method contains the logic for try-catching html extraction
            exceptions to be used with inline list comprehension.
            """
            try:
                summary = listing_card_element.find("div", {"class":"summary"}).ul.text.strip("\n")
            except:
                summary = None
            return summary
        
        # Adding Authorization Headers to the requests:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        r = requests.get(indeed_url, headers=headers)
        soup = bs4.BeautifulSoup(r.text, "html.parser")
        listings = soup.find_all("div", {"class":"jobsearch-SerpJobCard"})
        
        # Extracting the job postings data:
        jobs = [
            {
                "id": listing_card["id"],
                "title":listing_card.find("h2", {"class":"title"}).a.text.strip('\n'),
                "company": get_company_name(listing_card),
                "location":listing_card.find("span", {"class":"location"}).text.strip('\n'),
                "summary":get_summary(listing_card),
                "date_posted":self._get_post_date(listing_card.find("span", {"class":"date"}).text.strip("\n"))
            }
            for listing_card in listings]
        
        # Extracting and Building the url to the next indeed webpage:
        try:
            next_page_href = soup.find("a", {"aria-label":"Next"})['href']
        except Exception:
            next_page_href = None
        next_link_page = f"https://ca.indeed.com{next_page_href}"

        return [jobs, next_link_page]

    def _extract_indeed_jobs(self, url, page_num, max_pages, job_listings=[]):
        """This method recursivley performs requests to indeed.ca to
        extract job listings for a specific job and area. 
        
        The method builds a url for indeed.ca based on the input 
        params. It then performs get requests to the indeed url to
        extract all job listings for the page. The method then extracts
        the url for the "next" job listings page.
        
        This next url is then recursively passed into the same method that
        continues extracting job listings from the new page. This process
        recursivley continues for an arbitrary amount of time determined
        by an input param.
        
        Args:
        
        Returns:

        """
        if page_num > max_pages:
            # Print debug messages abount function execution:
            return job_listings
        
        else:
            # Extracing all data from the indeed url: 
            indeed_data = self._get_listings_and_next_link(url)
            
            # Adding job listings data to the list:
            job_listings.extend(indeed_data[0])
            
            # Second Exit Condition if no next url is found:
            if indeed_data[1] is None: 
                return job_listings

            # Waiting time to avoid throttling:
            time.sleep(5)

            # Recursivley calling function:
            return self._extract_indeed_jobs(indeed_data[1], page_num+1, max_pages, job_listings)
