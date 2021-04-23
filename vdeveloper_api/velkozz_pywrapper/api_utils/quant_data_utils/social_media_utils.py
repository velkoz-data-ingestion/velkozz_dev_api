# Wallstreet Bets Ticker Count Frequency Method:
def build_wsb_ticker_freq(wsb_posts_df, *args):
    """Method converts structured timeseries data about wallstreetbets
    posts into a dict containing frequency counts of tickers mentioned
    in said posts.
    
    The method is designed to ingest data directly from the velkozz web
    api from the query api's '.get_subreddit_data()' method. It takes this
    dataframe and applys the following transformations to it:
    
    - Extracts frequency counts of all ticker mentions in the title and the
        body of all posts.
    - Resample all frequency counts by day.
    - Transform data into a dict in the form {"Timestamp":{dict of ticker frequency counts}}
    
    The method compares extracted ticker symbols from wsb posts that match with tickers
    found in lists of avaliable ticker symbols passed in as args. If none are provided
    the method will extract all 1-4 length capitalized strings, which will produce 
    unusable data.
    
    Args: 
        wsb_posts_df (pandas.DataFrame): The dataframe containing timeseries data 
            about r/wsb posts. 
            
        args (list): Method expects a list of ticker symbol lists. 
            
    Returns: 
        dict: The dictionary containing ticker frequency counts.
    """
    # Nested function to extract tickers from abody of text:
    def get_ticker(text):
    
        # Splitting the string into list of strings:
        words = [text.replace("$", "") for text in text.split()]    
        
        # Flattening the list args into single list:
        listed_tickers = list(itertools.chain(*args))
        #print(listed_tickers)
        
        # Extracting only length 1-4 captialized strings:
        tickers = set([
            word for word in words 
            if 1 < len(word) <= 4 
            and word.isupper() 
            and word in listed_tickers
        ])
                    
        return tickers 
    
    # Slicing dataframe to contain only relevant data:
    post_content_df = wsb_posts_df[["title", "content", "created_on"]]
    post_content_df.set_index("created_on", inplace=True)
    
    # Resetting Index to a datetime object:
    post_content_df.index = pd.to_datetime(post_content_df.index)
    
    # Creating columns containing unique ticker mentions:
    post_content_df["title_tickers"] = post_content_df["title"].apply(lambda x: get_ticker(x))
    post_content_df["content_tickers"] = post_content_df["content"].apply(lambda x: get_ticker(x))
    
    # Splitting the dataframe into daily slices:
    data_dict = {}
    
    # Getting days listed by the dataframe:
    dayspan = post_content_df.index.floor("d").unique()
    
    for day in dayspan:
        date = datetime.strftime(day,'%Y-%m-%d')
        
        # Daily df slice:
        dayslice_df = post_content_df[date]
        
        # Extracing list of ticker variables:
        unique_title_tickers = [item for items in dayslice_df["title_tickers"].values for item in items]
        unqie_content_tickers = [item for items in dayslice_df["content_tickers"].values for item in items] 

        # Adding counters from title and content columns:
        ticker_collections = collections.Counter(unique_title_tickers) + collections.Counter(unqie_content_tickers)
        
        # Building the data_dict:
        data_dict[date] = ticker_collections
    
    return data_dict
