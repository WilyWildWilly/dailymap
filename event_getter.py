#!/usr/bin/env python3

# Attempt 5: create "circles" by querying gdelt geo with large ranges with center on different coordinates to cover earth



## Attempt 4

# """
# GDELT Natural Disaster Monitor
# Searches for natural disaster events with geographic coordinates using GDELT API
# """

# import requests
# import json
# import pandas as pd
# from datetime import datetime, timedelta
# import time
# from typing import List, Dict, Optional, Tuple
# import argparse

# class GDELTDisasterMonitor:
#     def __init__(self, max_records: int = 100, timeout: int = 30):
#         """
#         Initialize the GDELT monitor
        
#         Args:
#             max_records: Maximum number of records to retrieve per query
#             timeout: Request timeout in seconds
#         """
#         self.base_url = "https://api.gdeltproject.org/api/v2"
#         self.max_records = max_records
#         self.timeout = timeout
        
#         # Define natural disaster categories and their keywords
#         self.disaster_categories = {
#             'earthquake': ['earthquake', 'seismic', 'tremor', 'quake', 'magnitude', 'epicenter', 'aftershock'],
#             'flood': ['flood', 'flooding', 'inundation', 'deluge', 'flash flood', 'river overflow'],
#             'storm': ['hurricane', 'typhoon', 'cyclone', 'storm', 'gale', 'tempest', 'monsoon'],
#             'wildfire': ['wildfire', 'bushfire', 'forest fire', 'wildland fire', 'brush fire', 'firestorm'],
#             'landslide': ['landslide', 'mudslide', 'rockslide', 'landslip', 'debris flow', 'avalanche'],
#             'tsunami': ['tsunami', 'tidal wave', 'seismic sea wave'],
#             'volcano': ['volcano', 'eruption', 'volcanic', 'lava', 'magma', 'ash cloud'],
#             'tornado': ['tornado', 'twister', 'funnel cloud'],
#             'drought': ['drought', 'water shortage', 'aridity', 'dry spell'],
#             'heatwave': ['heatwave', 'heat wave', 'extreme heat', 'heatstroke'],
#             'cold': ['cold snap', 'blizzard', 'snowstorm', 'ice storm', 'freeze', 'frost'],
#             'other': ['natural disaster', 'catastrophe', 'calamity', 'cataclysm']
#         }
    
#     def get_geo_events(self, event_type: str = None, hours: int = 2, 
#                        location: Tuple[float, float] = None, radius_km: int = None) -> Dict:
#         """
#         Get natural disaster events with geographic coordinates using GDELT Geo API
        
#         Args:
#             event_type: Specific disaster type or None for all natural disasters
#             hours: Number of hours to look back
#             location: Tuple of (latitude, longitude) for center point
#             radius_km: Search radius in kilometers
        
#         Returns:
#             Dictionary with event data including coordinates
#         """
#         url = f"{self.base_url}/geo/geo"
        
#         # Build query based on event type
#         if event_type and event_type in self.disaster_categories:
#             # Use specific disaster type
#             query_keywords = self.disaster_categories[event_type]
#             query = f'({" OR ".join(query_keywords)})'
#         elif event_type:
#             # Use custom event type
#             query = event_type
#         else:
#             # Combine all disaster categories
#             all_keywords = []
#             for keywords in self.disaster_categories.values():
#                 all_keywords.extend(keywords[:3])  # Take top 3 keywords from each category
#             query = f'({" OR ".join(all_keywords)})'
        
#         # Add severity/impact keywords
#         query += " (death OR fatal OR casualties OR damage OR destroyed OR emergency OR evacuation)"
        
#         params = {
#             "query": query,
#             "mode": "pointdata",  # Returns points with coordinates
#             "format": "json",
#             "timespan": f"{hours}h",
#             "maxpoints": self.max_records
#         }
        
#         # Add location filter if specified
#         if location and radius_km:
#             params["location"] = f"{location[0]},{location[1]}"
#             params["radius"] = f"{radius_km}km"
        
#         print(f"Querying GDELT Geo API with: {query}")
#         print(f"Time range: Last {hours} hours")
#         if location:
#             print(f"Location: {location}, Radius: {radius_km}km")
        
#         try:
#             response = requests.get(url, params=params, timeout=self.timeout)
#             response.raise_for_status()
            
#             data = response.json()
#             return self._process_geo_data(data, event_type)
            
#         except requests.exceptions.RequestException as e:
#             print(f"Error querying GDELT API: {e}")
#             return {"error": str(e), "points": []}
#         except json.JSONDecodeError as e:
#             print(f"Error parsing JSON response: {e}")
#             return {"error": "Invalid JSON response", "points": []}
    
#     def _process_geo_data(self, data: Dict, event_type: str = None) -> Dict:
#         """
#         Process raw geo data into structured format
        
#         Args:
#             data: Raw JSON response from GDELT Geo API
#             event_type: The type of disaster searched for
        
#         Returns:
#             Processed data dictionary
#         """
#         processed_data = {
#             "metadata": {
#                 "query_time": datetime.now().isoformat(),
#                 "total_points": len(data.get("points", [])),
#                 "event_type": event_type or "all natural disasters"
#             },
#             "points": []
#         }
        
#         for point in data.get("points", []):
#             # Extract coordinates
#             coordinates = point.get("coordinates", {})
#             lat = coordinates.get("lat")
#             lon = coordinates.get("lng")
            
#             if lat is None or lon is None:
#                 continue  # Skip points without coordinates
            
#             # Determine event type from keywords
#             point_event_type = self._categorize_point(point)
            
#             # Extract relevant information
#             processed_point = {
#                 "latitude": lat,
#                 "longitude": lon,
#                 "event_type": point_event_type,
#                 "num_articles": point.get("numarticles", 0),
#                 "avg_tone": point.get("avgtone", 0),
#                 "urls": point.get("urls", []),
#                 "title": point.get("title", ""),
#                 "location_name": point.get("locationname", ""),
#                 "country_code": point.get("countrycode", ""),
#                 "adm1_code": point.get("adm1code", ""),  # State/Province code
#                 "confidence": point.get("confidence", 0),
#                 "date_added": point.get("dateadded", "")
#             }
            
#             # Add keywords if available
#             if "keywords" in point:
#                 processed_point["keywords"] = point.get("keywords", [])
            
#             processed_data["points"].append(processed_point)
        
#         return processed_data
    
#     def _categorize_point(self, point: Dict) -> str:
#         """
#         Categorize a point into specific disaster type based on keywords
        
#         Args:
#             point: Geo point data from GDELT
        
#         Returns:
#             Disaster category string
#         """
#         # Check title and keywords
#         search_text = ""
#         if "title" in point:
#             search_text += point["title"].lower() + " "
#         if "keywords" in point:
#             search_text += " ".join(point["keywords"]).lower() + " "
        
#         # Check each category
#         for category, keywords in self.disaster_categories.items():
#             for keyword in keywords:
#                 if keyword in search_text:
#                     return category
        
#         return "other"
    
#     def get_articles_for_event(self, event_type: str, hours: int = 2) -> List[Dict]:
#         """
#         Get detailed articles for a specific event type using Document API
        
#         Args:
#             event_type: Type of disaster to search for
#             hours: Hours to look back
        
#         Returns:
#             List of article dictionaries
#         """
#         url = f"{self.base_url}/doc/doc"
        
#         if event_type in self.disaster_categories:
#             keywords = self.disaster_categories[event_type]
#             query = f'({" OR ".join(keywords)})'
#         else:
#             query = event_type
        
#         params = {
#             "query": f'{query} (death OR fatal OR casualties OR emergency)',
#             "mode": "artlist",
#             "format": "json",
#             "maxrecords": min(self.max_records, 250),  # Document API max is 250
#             "timespan": f"{hours}h",
#             "sort": "datedesc"
#         }
        
#         try:
#             response = requests.get(url, params=params, timeout=self.timeout)
#             response.raise_for_status()
            
#             data = response.json()
#             return data.get("articles", [])
            
#         except requests.exceptions.RequestException as e:
#             print(f"Error querying Document API: {e}")
#             return []
    
#     def search_by_time_range(self, start_time: str, end_time: str, 
#                             event_type: str = None) -> Dict:
#         """
#         Search for events in a specific time range
        
#         Args:
#             start_time: Start time in format YYYYMMDDHHMMSS
#             end_time: End time in format YYYYMMDDHHMMSS
#             event_type: Optional specific disaster type
        
#         Returns:
#             Dictionary with events in the time range
#         """
#         timespan = f"{start_time}-{end_time}"
        
#         url = f"{self.base_url}/geo/geo"
        
#         if event_type and event_type in self.disaster_categories:
#             keywords = self.disaster_categories[event_type]
#             query = f'({" OR ".join(keywords)})'
#         else:
#             query = " OR ".join([k for keywords in self.disaster_categories.values() 
#                                for k in keywords[:2]])
        
#         query += " (emergency OR disaster OR casualties)"
        
#         params = {
#             "query": query,
#             "mode": "pointdata",
#             "format": "json",
#             "timespan": timespan,
#             "maxpoints": self.max_records
#         }
        
#         try:
#             response = requests.get(url, params=params, timeout=self.timeout)
#             response.raise_for_status()
            
#             data = response.json()
#             return self._process_geo_data(data, event_type)
            
#         except requests.exceptions.RequestException as e:
#             print(f"Error querying API: {e}")
#             return {"error": str(e), "points": []}
    
#     def export_to_csv(self, data: Dict, filename: str = "gdelt_disasters.csv"):
#         """
#         Export event data to CSV file
        
#         Args:
#             data: Processed event data dictionary
#             filename: Output CSV filename
#         """
#         if "points" not in data or not data["points"]:
#             print("No data to export")
#             return
        
#         df = pd.DataFrame(data["points"])
        
#         # Reorder columns for better readability
#         column_order = [
#             'event_type', 'title', 'location_name', 'country_code',
#             'latitude', 'longitude', 'num_articles', 'avg_tone',
#             'date_added', 'confidence', 'urls'
#         ]
        
#         # Only include columns that exist
#         existing_columns = [col for col in column_order if col in df.columns]
#         other_columns = [col for col in df.columns if col not in column_order]
        
#         df = df[existing_columns + other_columns]
#         df.to_csv(filename, index=False, encoding='utf-8')
#         print(f"Exported {len(df)} events to {filename}")
    
#     def print_summary(self, data: Dict):
#         """
#         Print a summary of the event data
        
#         Args:
#             data: Processed event data dictionary
#         """
#         if "points" not in data:
#             print("No data available")
#             return
        
#         points = data["points"]
#         metadata = data.get("metadata", {})
        
#         print("\n" + "="*80)
#         print("GDELT NATURAL DISASTER MONITOR - SUMMARY")
#         print("="*80)
        
#         print(f"\nQuery Time: {metadata.get('query_time', 'N/A')}")
#         print(f"Event Type: {metadata.get('event_type', 'N/A')}")
#         print(f"Total Events Found: {len(points)}")
        
#         if not points:
#             print("\nNo events found in the specified time period.")
#             return
        
#         # Count by event type
#         event_counts = {}
#         for point in points:
#             event_type = point.get("event_type", "unknown")
#             event_counts[event_type] = event_counts.get(event_type, 0) + 1
        
#         print("\nEvents by Type:")
#         for event_type, count in sorted(event_counts.items(), key=lambda x: x[1], reverse=True):
#             print(f"  {event_type}: {count}")
        
#         # Most reported events
#         print(f"\nTop 5 Most Reported Events:")
#         sorted_points = sorted(points, key=lambda x: x.get("num_articles", 0), reverse=True)
#         for i, point in enumerate(sorted_points[:5], 1):
#             print(f"\n{i}. {point.get('event_type', 'Unknown').upper()}")
#             print(f"   Location: {point.get('location_name', 'Unknown')} "
#                   f"({point.get('latitude', 'N/A')}, {point.get('longitude', 'N/A')})")
#             print(f"   Articles: {point.get('num_articles', 0)}, "
#                   f"Avg Tone: {point.get('avg_tone', 0):.2f}")
#             if point.get('title'):
#                 print(f"   Title: {point.get('title', '')[:80]}...")
        
#         # Geographic distribution
#         countries = {}
#         for point in points:
#             country = point.get("country_code", "Unknown")
#             if country:
#                 countries[country] = countries.get(country, 0) + 1
        
#         if len(countries) > 0:
#             print(f"\nGeographic Distribution ({len(countries)} countries/regions):")
#             for country, count in sorted(countries.items(), key=lambda x: x[1], reverse=True)[:10]:
#                 print(f"  {country}: {count}")
        
#         print("\n" + "="*80)

# def main():
#     """Main function with command line interface"""
#     parser = argparse.ArgumentParser(description="Monitor natural disasters using GDELT API")
#     parser.add_argument("--hours", type=int, default=2, help="Hours to look back (default: 2)")
#     parser.add_argument("--type", type=str, help="Specific disaster type (e.g., earthquake, flood)")
#     parser.add_argument("--location", type=str, help="Center location as lat,lon (e.g., 35.68,139.76)")
#     parser.add_argument("--radius", type=int, help="Search radius in kilometers")
#     parser.add_argument("--export", type=str, help="Export to CSV filename")
#     parser.add_argument("--list-types", action="store_true", help="List available disaster types")
    
#     args = parser.parse_args()
    
#     monitor = GDELTDisasterMonitor(max_records=100)
    
#     if args.list_types:
#         print("Available disaster types:")
#         for category in monitor.disaster_categories.keys():
#             print(f"  - {category}")
#         return
    
#     # Parse location if provided
#     location_tuple = None
#     if args.location:
#         try:
#             lat, lon = map(float, args.location.split(','))
#             location_tuple = (lat, lon)
#         except ValueError:
#             print(f"Invalid location format: {args.location}. Use 'lat,lon'")
#             return
    
#     # Get events
#     print("Fetching natural disaster events from GDELT...")
    
#     if location_tuple and args.radius:
#         data = monitor.get_geo_events(
#             event_type=args.type,
#             hours=args.hours,
#             location=location_tuple,
#             radius_km=args.radius
#         )
#     else:
#         data = monitor.get_geo_events(
#             event_type=args.type,
#             hours=args.hours
#         )
    
#     # Print summary
#     monitor.print_summary(data)
    
#     # Export if requested
#     if args.export and "points" in data and data["points"]:
#         monitor.export_to_csv(data, args.export)
    
#     # Example of getting detailed articles for the most common event type
#     if "points" in data and data["points"]:
#         # Find most common event type
#         event_types = [p.get("event_type") for p in data["points"]]
#         if event_types:
#             from collections import Counter
#             most_common = Counter(event_types).most_common(1)[0][0]
#             print(f"\nGetting detailed articles for {most_common} events...")
#             articles = monitor.get_articles_for_event(most_common, hours=args.hours)
#             print(f"Found {len(articles)} articles about {most_common}")

# if __name__ == "__main__":
#     main()




## Attempt 3

# import os
# from google.cloud import bigquery

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/Users/gchela/.config/gcloud/application_default_credentials.json'

# client = bigquery.Client()

# query = """
# SELECT 
#     SQLDate,
#     DATEADDED,
#     SOURCEURL,
#     ActionGeo_CountryCode,
#     ActionGeo_Lat,
#     ActionGeo_Long,
#     Actor1Name,
#     Actor2Name,
#     EventCode,
#     EventBaseCode,
#     EventRootCode,
#     GoldsteinScale,
#     NumArticles,
#     AvgTone
# FROM `gdelt-bq.gdeltv2.events`
# WHERE 
#     -- Date filter (using a real past date)
#     SQLDATE >= 20251229090000 AND SQLDATE <= 20251229110000
#     -- Natural disaster event codes (Root code 17)
#     AND EventRootCode = '17'
#     -- Filter for deadly/disastrous events
#     AND (
#         EventCode IN ('171', '172', '173', '174') OR
#         Actor1Name LIKE '%earthquake%' OR
#         Actor1Name LIKE '%tsunami%' OR
#         Actor1Name LIKE '%flood%' OR
#         Actor1Name LIKE '%hurricane%' OR
#         Actor1Name LIKE '%tornado%' OR
#         Actor1Name LIKE '%volcano%' OR
#         Actor1Name LIKE '%wildfire%'
#     )
#     -- Filter for negative/impactful events
#     AND AvgTone < -2.0
# ORDER BY NumArticles DESC
# LIMIT 50
# """

# query_job = client.query(query)
# results = query_job.result()

# for row in results:
#     print(f"Date: {row.SQLDate}, Event: {row.EventCode}, Articles: {row.NumArticles}")

## Attempt 2

# import gdelt
# import json

# gd= gdelt.gdelt(version=2)
# events = gd.Search(['2025 12 29','2025 12 29'],table='events',output='json',normcols=True,coverage=False)

# events.info()


## Attempt 1

# """
# GDELT Event Database Query Script for Natural Disaster Events
# Queries events from the last 8 hours for specified disaster themes
# """

# import requests
# import pandas as pd
# from datetime import datetime, timedelta
# import json
# import time
# from typing import List, Dict, Optional, Tuple
# import argparse
# import urllib.parse

# class GDELTEventQuery:
#     def __init__(self):
#         self.base_url = "https://api.gdeltproject.org/api/v2/event/event"
#         # Using CAMEO event codes and themes for natural disasters
#         self.disaster_codes = {
#             # Environmental disasters (CAMEO Event Codes)
#             'Earthquake': ['190'],  # Natural disasters
#             'Flood': ['191'],
#             'Storm': ['192', '193'],  # Heavy rains, storms
#             'Fire': ['194'],  # Wildfire, forest fire
#             'Drought': ['195'],
#             'Volcano': ['196'],
#             'Cold Wave': ['197'],
#             'Heat Wave': ['198'],
#             # Additional natural disaster event codes
#             'Hurricane': ['192', '193'],  # Storms/cyclones
#             'Tsunami': ['190', '191'],  # Often follows earthquakes
#             'Landslide': ['190', '191'],  # Often follows floods/earthquakes
#             'Avalanche': ['197'],  # Cold/storm related
#         }
        
#         # Alternative: Query by theme using GDELT 2.0 theme codes
#         self.disaster_themes = {
#             "NATURAL_DISASTER": "NATURAL_DISASTER",
#             "DISASTER_GEOPHYSICAL": "DISASTER_GEOPHYSICAL",  # Earthquakes, tsunamis, volcanoes
#             "DISASTER_METEOROLOGICAL": "DISASTER_METEOROLOGICAL",  # Storms, hurricanes, cyclones
#             "DISASTER_HYDROLOGICAL": "DISASTER_HYDROLOGICAL",  # Floods, landslides
#             "DISASTER_CLIMATOLOGICAL": "DISASTER_CLIMATOLOGICAL",  # Droughts, wildfires, heatwaves
#             "DISASTER_BIOLOGICAL": "DISASTER_BIOLOGICAL",  # Epidemics, insect infestations
#         }
    
#     def get_time_window(self, hours_back: int = 8) -> Tuple[str, str]:
#         """
#         Calculate the time window for the query
#         Returns: (startdate, enddate) in YYYY-MM-DD HH:MM:SS format
#         """
#         end_time = datetime.utcnow()
#         start_time = end_time - timedelta(hours=hours_back)
        
#         start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
#         end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
        
#         return start_str, end_str
    
#     def build_event_code_query(self, disaster_types: Optional[List[str]] = None) -> str:
#         """
#         Build query using CAMEO event codes
#         """
#         if disaster_types is None:
#             disaster_types = list(self.disaster_codes.keys())
        
#         event_codes = []
#         for disaster in disaster_types:
#             if disaster in self.disaster_codes:
#                 event_codes.extend(self.disaster_codes[disaster])
        
#         # Remove duplicates
#         event_codes = list(set(event_codes))
        
#         # Create query string
#         if event_codes:
#             query = " OR ".join([f"eventcode:{code}" for code in event_codes])
#             return f"({query})"
        
#         return ""
    
#     def build_theme_query(self, themes: Optional[List[str]] = None) -> str:
#         """
#         Build query using GDELT 2.0 themes
#         """
#         if themes is None:
#             themes = list(self.disaster_themes.keys())
        
#         theme_queries = []
#         for theme in themes:
#             if theme in self.disaster_themes:
#                 theme_queries.append(f"theme:{self.disaster_themes[theme]}")
        
#         if theme_queries:
#             query = " OR ".join(theme_queries)
#             return f"({query})"
        
#         return ""
    
#     def build_query(self, query_type: str = "theme", 
#                    disaster_types: Optional[List[str]] = None,
#                    themes: Optional[List[str]] = None) -> str:
#         """
#         Build the complete GDELT query string
#         """
#         if query_type == "eventcode":
#             query = self.build_event_code_query(disaster_types)
#         else:  # theme query
#             query = self.build_theme_query(themes)
        
#         if not query:
#             query = "theme:NATURAL_DISASTER"
        
#         # Get time window
#         start_time, end_time = self.get_time_window(8)
        
#         # Build complete query
#         full_query = f"{query}&startdatetime={start_time}&enddatetime={end_time}&format=json"
        
#         return full_query
    
#     def query_gdelt_events(self, query_string: str, max_records: int = 250) -> Optional[Dict]:
#         """
#         Query the GDELT Event API
#         """
#         try:
#             # URL encode the query
#             encoded_query = urllib.parse.quote(query_string, safe='&=')  # Changed from quote_plus
            
#             full_url = f"{self.base_url}?query={encoded_query}&maxrecords={max_records}"
            
#             print(f"Querying GDELT Event API...")
#             print(f"URL: {full_url[:200]}...")  # Truncate for display
            
#             # Add headers to avoid potential blocking
#             headers = {
#                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
#                 'Accept': 'application/json'
#             }
            
#             response = requests.get(full_url, headers=headers, timeout=30)
#             response.raise_for_status()
            
#             data = response.json()
            
#             # Check if we got a valid response
#             if 'events' in data:
#                 return data
#             else:
#                 print(f"Unexpected response format: {list(data.keys()) if isinstance(data, dict) else type(data)}")
#                 return None
            
#         except requests.exceptions.RequestException as e:
#             print(f"Error querying GDELT API: {e}")
#             return None
#         except json.JSONDecodeError as e:
#             print(f"Error parsing JSON response: {e}")
#             print(f"Response text: {response.text[:500] if 'response' in locals() else 'No response'}")
#             return None
    
#     def parse_event_results(self, data: Dict) -> pd.DataFrame:
#         """
#         Parse the GDELT Event response into a pandas DataFrame
#         """
#         if not data or 'events' not in data:
#             return pd.DataFrame()
        
#         events = data['events']
#         records = []
        
#         for event in events:
#             # Parse actors
#             actor1 = event.get('actor1', {})
#             actor2 = event.get('actor2', {})
            
#             # Parse geo information
#             geo1 = event.get('geo', {}).get('actor1', {})
#             geo2 = event.get('geo', {}).get('actor2', {})
#             action_geo = event.get('geo', {}).get('action', {})
            
#             # Parse date
#             event_date = event.get('dateadded', '')
#             if event_date and len(event_date) >= 8:
#                 try:
#                     date_obj = datetime.strptime(event_date[:8], "%Y%m%d")
#                     event_date = date_obj.strftime("%Y-%m-%d")
#                 except:
#                     pass
            
#             record = {
#                 # Event identification
#                 'globaleventid': event.get('globaleventid', ''),
#                 'event_id': event.get('eventid', ''),
#                 'date': event_date,
#                 'month': event.get('month', ''),
#                 'year': event.get('year', ''),
                
#                 # Event information
#                 'event_code': event.get('eventcode', ''),
#                 'event_root_code': event.get('eventrootcode', ''),
#                 'event_base_code': event.get('eventbasecode', ''),
#                 'goldstein_scale': event.get('goldsteinscale', 0),
#                 'num_articles': event.get('numarticles', 1),
#                 'avg_tone': event.get('avgtone', 0),
                
#                 # Actor 1 (source)
#                 'actor1_name': actor1.get('name', ''),
#                 'actor1_country_code': actor1.get('countrycode', ''),
#                 'actor1_type': actor1.get('type', ''),
                
#                 # Actor 2 (target)
#                 'actor2_name': actor2.get('name', ''),
#                 'actor2_country_code': actor2.get('countrycode', ''),
#                 'actor2_type': actor2.get('type', ''),
                
#                 # Geographic information
#                 'action_country': action_geo.get('countrycode', ''),
#                 'action_lat': action_geo.get('lat', ''),
#                 'action_lng': action_geo.get('lng', ''),
#                 'actor1_country': geo1.get('countrycode', ''),
#                 'actor2_country': geo2.get('countrycode', ''),
                
#                 # URL for source articles
#                 'source_url': event.get('sourceurl', ''),
#             }
            
#             # Add themes if present
#             if 'themes' in event and event['themes']:
#                 record['themes'] = ', '.join(event['themes'])
#             else:
#                 record['themes'] = ''
            
#             records.append(record)
        
#         return pd.DataFrame(records)
    
#     def get_event_code_description(self, event_code: str) -> str:
#         """
#         Get description for CAMEO event codes
#         """
#         event_descriptions = {
#             '190': 'Natural Disaster (General)',
#             '191': 'Flood',
#             '192': 'Heavy Rains',
#             '193': 'Storm/Cyclone/Hurricane/Typhoon',
#             '194': 'Fire/Wildfire',
#             '195': 'Drought',
#             '196': 'Volcano',
#             '197': 'Cold Wave',
#             '198': 'Heat Wave',
#         }
        
#         return event_descriptions.get(event_code, f'Event Code {event_code}')
    
#     def analyze_results(self, df: pd.DataFrame):
#         """
#         Analyze and display summary of event results
#         """
#         if df.empty:
#             print("\nNo events found for the specified criteria and time period.")
#             return
        
#         print(f"\n{'='*60}")
#         print(f"EVENT QUERY RESULTS SUMMARY")
#         print(f"{'='*60}")
#         print(f"Total events found: {len(df)}")
#         start_time, end_time = self.get_time_window(8)
#         print(f"Time period: {start_time} to {end_time} UTC")
#         print(f"{'='*60}")
        
#         # Count by event type
#         print("\nEvents by type:")
#         if 'event_code' in df.columns and not df.empty:
#             event_counts = df['event_code'].value_counts()
#             for code, count in event_counts.head(10).items():
#                 description = self.get_event_code_description(str(code))
#                 print(f"  {description} (Code {code}): {count} events")
        
#         # Count by country
#         if 'action_country' in df.columns and not df['action_country'].empty:
#             country_counts = df['action_country'].value_counts()
#             print(f"\nTop countries where events occurred:")
#             for country, count in country_counts.head(10).items():
#                 if country and str(country) != 'nan':
#                     print(f"  {country}: {count} events")
        
#         # Count by source actor
#         if 'actor1_name' in df.columns and not df['actor1_name'].empty:
#             actor_counts = df['actor1_name'].value_counts()
#             print(f"\nTop source actors (reporting):")
#             for actor, count in actor_counts.head(5).items():
#                 if actor and str(actor) != 'nan':
#                     print(f"  {actor}: {count} events")
        
#         # Statistical summary
#         print(f"\nStatistical Summary:")
#         print(f"  Average Goldstein Scale: {df['goldstein_scale'].mean():.2f}")
#         print(f"  Average Tone: {df['avg_tone'].mean():.2f}")
#         print(f"  Average Articles per Event: {df['num_articles'].mean():.1f}")
        
#         # Timeline
#         if 'date' in df.columns and not df['date'].empty:
#             print(f"\nEvent Timeline:")
#             date_counts = df['date'].value_counts().sort_index()
#             for date, count in date_counts.head(5).items():
#                 if date:
#                     print(f"  {date}: {count} events")
#             if len(date_counts) > 5:
#                 print(f"  ... and {len(date_counts) - 5} more days")
    
#     def save_results(self, df: pd.DataFrame, output_format: str = 'csv'):
#         """
#         Save results to file
#         """
#         if df.empty:
#             print("No data to save.")
#             return
        
#         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
#         if output_format.lower() == 'csv':
#             filename = f"gdelt_events_natural_disasters_{timestamp}.csv"
#             df.to_csv(filename, index=False, encoding='utf-8')
#             print(f"\nResults saved to: {filename}")
        
#         elif output_format.lower() == 'excel':
#             filename = f"gdelt_events_natural_disasters_{timestamp}.xlsx"
#             df.to_excel(filename, index=False)
#             print(f"\nResults saved to: {filename}")
        
#         elif output_format.lower() == 'json':
#             filename = f"gdelt_events_natural_disasters_{timestamp}.json"
#             df.to_json(filename, orient='records', indent=2)
#             print(f"\nResults saved to: {filename}")
        
#         else:
#             print(f"Unknown format: {output_format}. Using CSV.")
#             filename = f"gdelt_events_natural_disasters_{timestamp}.csv"
#             df.to_csv(filename, index=False, encoding='utf-8')
#             print(f"\nResults saved to: {filename}")
    
#     def display_event_sample(self, df: pd.DataFrame, sample_size: int = 5):
#         """
#         Display sample events in a readable format
#         """
#         if df.empty:
#             return
        
#         print(f"\n{'='*60}")
#         print(f"SAMPLE EVENTS (first {min(sample_size, len(df))} events)")
#         print(f"{'='*60}")
        
#         for idx, row in df.head(sample_size).iterrows():
#             print(f"\nEvent {idx + 1}:")
#             print(f"  Event ID: {row.get('globaleventid', 'N/A')}")
            
#             # Get event description
#             event_code = str(row.get('event_code', ''))
#             event_desc = self.get_event_code_description(event_code)
#             print(f"  Type: {event_desc}")
            
#             print(f"  Date: {row.get('date', 'N/A')}")
#             print(f"  Location: {row.get('action_country', 'N/A')}")
            
#             # Actors
#             actor1 = row.get('actor1_name', '')
#             actor2 = row.get('actor2_name', '')
#             if actor1 and actor2:
#                 print(f"  Actors: {actor1} -> {actor2}")
#             elif actor1:
#                 print(f"  Actor: {actor1}")
            
#             # Event metrics
#             print(f"  Goldstein Scale: {row.get('goldstein_scale', 'N/A')}")
#             print(f"  Average Tone: {row.get('avg_tone', 'N/A')}")
#             print(f"  Articles: {row.get('num_articles', 'N/A')}")
            
#             # Themes
#             themes = row.get('themes', '')
#             if themes:
#                 print(f"  Themes: {themes[:100]}...")
            
#             # Source
#             source = row.get('source_url', '')
#             if source:
#                 print(f"  Source: {source[:80]}...")

# def main():
#     parser = argparse.ArgumentParser(description='Query GDELT Event Database for natural disaster events')
#     parser.add_argument('--query-type', choices=['theme', 'eventcode'], default='theme',
#                        help='Query by theme or event code (default: theme)')
    
#     # For event code queries
#     parser.add_argument('--disaster-types', nargs='+', 
#                        choices=['Earthquake', 'Flood', 'Storm', 'Fire', 'Drought', 
#                                 'Volcano', 'Cold Wave', 'Heat Wave', 'Hurricane', 
#                                 'Tsunami', 'Landslide', 'Avalanche'],
#                        help='Specific disaster types to query (for event code queries)')
    
#     # For theme queries
#     parser.add_argument('--themes', nargs='+', 
#                        choices=['NATURAL_DISASTER', 'DISASTER_GEOPHYSICAL', 
#                                 'DISASTER_METEOROLOGICAL', 'DISASTER_HYDROLOGICAL',
#                                 'DISASTER_CLIMATOLOGICAL', 'DISASTER_BIOLOGICAL'],
#                        help='Specific themes to query (for theme queries)')
    
#     parser.add_argument('--output', choices=['csv', 'excel', 'json'], default='csv',
#                        help='Output format (default: csv)')
#     parser.add_argument('--max-records', type=int, default=250,
#                        help='Maximum records to retrieve (default: 250)')
#     parser.add_argument('--hours', type=int, default=8,
#                        help='Hours to look back (default: 8)')
#     parser.add_argument('--sample', type=int, default=5,
#                        help='Number of sample events to display (default: 5)')
    
#     args = parser.parse_args()
    
#     # Create query instance
#     gdelt = GDELTEventQuery()
    
#     # Modify time window if specified
#     if args.hours != 8:
#         original_method = gdelt.get_time_window
#         gdelt.get_time_window = lambda: original_method(args.hours)
    
#     # Build query based on query type
#     if args.query_type == 'eventcode':
#         query_string = gdelt.build_query(
#             query_type='eventcode',
#             disaster_types=args.disaster_types
#         )
#     else:  # theme query
#         query_string = gdelt.build_query(
#             query_type='theme',
#             themes=args.themes
#         )
    
#     # Execute query
#     print(f"\nBuilding query for natural disaster events...")
#     print(f"Query type: {args.query_type}")
#     print(f"Time window: Last {args.hours} hours")
    
#     data = gdelt.query_gdelt_events(query_string, args.max_records)
    
#     if data:
#         # Parse results
#         df = gdelt.parse_event_results(data)
        
#         # Display analysis
#         gdelt.analyze_results(df)
        
#         # Save results
#         gdelt.save_results(df, args.output)
        
#         # Display sample events
#         gdelt.display_event_sample(df, args.sample)
        
#     else:
#         print("\nFailed to retrieve data from GDELT Event API.")
#         print("Possible issues:")
#         print("1. No events in the specified time period")
#         print("2. Network connectivity issues")
#         print("3. GDELT API rate limiting")
#         print("\nTry adjusting the time window or using different query parameters.")

# if __name__ == "__main__":
#     main()