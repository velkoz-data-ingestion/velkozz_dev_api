# Importing external packages:
import requests
import bonobo


class VelkozzAPI(object):

    def __init__(self, token, **kwargs):
        
        # Extracting kwargs and declaring instance params:
        self.token = token
        self.api_url = kwargs.get("URL", "http://localhost:8000/")
        self.username = kwargs.get("username", None)
        self.password = kwargs.get("password", None)

class Pipeline(object):
    """The Base Object representing an ETL pipeline.
    
    It contains all of the methods necessary to perform ETL functions through the 
    Bonobo library. It contains various blueprint methods that are intended to be 
    overwritten by Pipeline objects that extends the base class.

    The methods that are intended to be overwritten are:
    - extract()
    - transform()
    - load()
    
    The methods that can but do not need to be overwritten:
    - build_graph()
    - get_services()
    
    Arguments:
        kwargs (dict): The key word arguments used to configure inherited pipeline objects. 
            
    
    """
    def __init__(self, **kwargs):
        self.kwargs = kwargs 

    # <------Base Bonobo ETL Methods------->
    def extract(self):
        pass

    def transform(self, *args):
        pass
    
    def load(self, *args):
        pass

    def build_graph(self, **options):

        # Building the Graph:
        self.graph = bonobo.Graph()    
        self.graph.add_chain(
            self.extract,
            self.transform,
            self.load)

        return self.graph

    def get_services(self, **options):
        return {}
        
    # Executon method:
    def execute_pipeline(self):
        
        self.bonobo_parser = bonobo.get_argument_parser()
        with bonobo.parse_args(self.bonobo_parser) as options:
            bonobo.run(
                self.build_graph(**options),
                services=self.get_services(**options))



