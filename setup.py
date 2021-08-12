from setuptools import setup, find_namespace_packages

setup(
    name = "velkozz-developer-api",
    url = "https://github.com/velkoz-data-ingestion/velkozz_dev_api",
    version = "0.1.2",
    author = "Matthew Teelucksingh",
    packages = find_namespace_packages(
        include = ["vdeveloper_api.*"]
    ),
    install_requires = [
        "pandas", 
        "bonobo",
        "praw",
        "requests",
        "beautifulsoup4",
        "pytz",
        "google-api-python-client"
    ],
    license = 'MIT',
    long_description=open('README.md').read()   
    )