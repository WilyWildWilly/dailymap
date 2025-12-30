# import argparse
# import requests
import json
# import yfinance as yf
# import pandas as pd
import time
from datetime import datetime, timedelta
from db_utils import EventsDatabase
from typing import List, Dict, Optional

def dataToMap():
    # initialize database 
    # Connect to the db
    with sqlite3.connect('world_events.db') as conn:
    # create db cursor
    # run queries
    # commit changes
        queryEndTime = datetime.now()
        queryStartTime = queryEndTime - timedelta(hours=168)
        cursor = conn.cursor()
        cursor.execute('''
    	SELECT * FROM events WHERE (
        	id INTEGER PRIMARY KEY,
        	first_name TEXT NOT NULL,
        	last_name TEXT NOT NULL,
        	email TEXT UNIQUE NOT NULL,
        	phone TEXT,
        	num_orders INTEGER
    	);'''
        all_events = cursor.fetchall()
        print("All Events:")
        for event in all_events:
    	    print(event)
        cursor.close()