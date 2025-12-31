import sqlite3
import time
from datetime import datetime, timedelta
from db_utils import EventsDatabase
from typing import List, Dict, Optional


# Project: select all events from database and draw them on a map (pillow?)

def dataToMap():
    # Initialize database 
    # Connect to the db
    with sqlite3.connect('world_events.db') as conn:
        # Create db cursor
        queryEndTime = datetime.now()
        queryStartTime = queryEndTime - timedelta(hours=168)
        cursor = conn.cursor()
        
        # Fixed SQL query - removed the CREATE TABLE syntax from SELECT
        cursor.execute('''
            SELECT * FROM events 
            WHERE datetime BETWEEN ? AND ?
        ''', (queryStartTime, queryEndTime))
            
        
        all_events = cursor.fetchall()
        
        print("All Events:")
        print(f"Found {len(all_events)} events")
        for event in all_events:
            print(event)
        
        cursor.close()
    
    return all_events


import sqlite3

def list_tables():
    with sqlite3.connect('world_events.db') as conn:
        cursor = conn.cursor()
        
        # Query to get all table names
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table'
            ORDER BY name;
        """)
        
        tables = cursor.fetchall()
        
        print("Tables in database:")
        for table in tables:
            print(f"  - {table[0]}")
        
        # For each table, show its structure
        print("\nTable structures:")
        for table in tables:
            table_name = table[0]
            print(f"\n{table_name}:")
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            for col in columns:
                print(f"  {col[1]} ({col[2]})")
        
        cursor.close()


if __name__ == "__main__":
    dataToMap()
# if __name__ == "__main__":
#     list_tables()