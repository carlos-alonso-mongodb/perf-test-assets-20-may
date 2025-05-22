from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import json
import time
from logging import getLogger
import logging
import os
import sys


logger = getLogger(__name__)
logger.setLevel("INFO")

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
console_handler.setFormatter(console_formatter)

def get_query_path():
    # Create file handler
    main_module = sys.modules['__main__']
    main_file_path = getattr(main_module, '__file__', 'helper')
    # Remove the root folder and include the package path
    package_path = os.path.relpath(main_file_path, start=os.path.dirname(__file__))
    package_path_no_ext = os.path.splitext(package_path)[0]
    return package_path_no_ext.replace(os.sep, '.')

# Create file handler
log_filename = "results.log"

file_handler = logging.FileHandler(log_filename)

file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
file_handler.setFormatter(file_formatter)

# Add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

client = None

def get_logger():
    global logger
    return logger

def JSONEncoder(obj):
    """Custom JSON encoder for MongoDB ObjectId."""
    if isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, datetime):
        return "ISODate('" + obj.isoformat() + "')"
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def get_connection():
    global client
    if client is not None:
        return client
    
    # MongoDB Atlas connection URI
    uri = "mongodb+srv://carlosalonso:admin1234@basicinitialtests.0asff.mongodb.net/?retryWrites=true&w=majority&appName=BasicInitialTests"
    client = MongoClient(uri)
    # force the connection to be established
    try:
        client.admin.command('ping')
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

    return client

def execute_query(ns, query):
    connection = get_connection()
    db_name, coll_name = ns.split(".")
    collection = connection[db_name][coll_name]

    # Execute and time the query
    start_time = time.time()    
    results = list(collection.find(query))
    end_time = time.time()

    elapsed_time_ms = (end_time - start_time) * 1000

    logger.info(f"{ get_query_path() }. ns: { ns }. Query took {elapsed_time_ms:.2f} ms. Documents found: {len(results)}. query: {json.dumps(query, default=JSONEncoder)}")

def execute_aggregation(ns, pipeline):
    connection = get_connection()
    db_name, coll_name = ns.split(".")
    collection = connection[db_name][coll_name]

    # Execute and time the query
    start_time = time.time()    
    results = list(collection.aggregate(pipeline))
    end_time = time.time()

    elapsed_time_ms = (end_time - start_time) * 1000

    logger.info(f"{ get_query_path() }. ns: { ns }. Query took {elapsed_time_ms:.2f} ms. Documents found: {len(results)}. query: {json.dumps(pipeline, default=JSONEncoder)}")

def print_json(data):
    # Print the JSON data
    print(json.dumps(data, indent=4, default=JSONEncoder))