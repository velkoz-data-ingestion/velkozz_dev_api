from setuptools import setup, find_packages

setup(
    name = "velkozz-pipeline-api",
    url = "https://github.com/velkoz-data-ingestion/velkozz_pipeline_api",
    version = "0.1.1",
    author = "Matthew Teelucksingh",
    packages = find_packages(
        where = "src",
        include = ["social_media_pipelines", "*pipelines", "core_objects", "utils"]
    ),
    package_dir = {"" : "src"},
    install_requires = [
        "pandas", 
        "bonobo",
        "praw",
        "requests",
        "velkozz-pywrapper @ git+https://git@github.com/velkoz-data-ingestion/velkozz_web_api_pywrapper.git"

    ],
    license = 'MIT',
    long_description=open('README.md').read()   
    )