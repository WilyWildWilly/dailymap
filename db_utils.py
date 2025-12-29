import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any
import json

class EventsDatabase:
    def __init__(self, db_path: str = 'world_events.db'):
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """Establish database connection"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Enable dictionary-like access
        return self.conn
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def add_event(self, event_type: str, lat: float, lon: float, 
                  event_datetime: str, title: str = None, 
                  description: str = None, amount: float = None,
                  currency: str = 'USD', severity: str = None,
                  category: str = None, source: str = None) -> int:
        """
        Add a new event to the database
        
        Args:
            event_type: 'death', 'natural_disaster', 'financial_loss', 'financial_gain'
            lat: Latitude (-90 to 90)
            lon: Longitude (-180 to 180)
            event_datetime: Event datetime in 'YYYY-MM-DD HH:MM:SS' format
            title: Event title
            description: Detailed description
            amount: Financial amount (positive for gain, negative for loss)
            currency: Currency code (default: USD)
            severity: 'low', 'medium', 'high', 'critical'
            category: Event category
            source: Data source
        
        Returns:
            Event ID
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO events 
        (event_type, title, description, datetime, latitude, longitude, 
         amount, currency, severity, category, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (event_type, title, description, event_datetime, lat, lon,
              amount, currency, severity, category, source))
        
        event_id = cursor.lastrowid
        conn.commit()
        self.close()
        return event_id
    
    def get_events(self, start_date: str = None, end_date: str = None,
                   event_types: List[str] = None, min_lat: float = None,
                   max_lat: float = None, min_lon: float = None,
                   max_lon: float = None, limit: int = 100) -> List[Dict]:
        """
        Query events with filters
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            event_types: List of event types to filter
            min_lat/min_lon/max_lat/max_lon: Bounding box coordinates
            limit: Maximum number of results
        
        Returns:
            List of event dictionaries
        """
        conn = self.connect()
        cursor = conn.cursor()
        
        query = "SELECT * FROM events WHERE 1=1"
        params = []
        
        if start_date:
            query += " AND datetime >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND datetime <= ?"
            params.append(end_date)
        
        if event_types:
            placeholders = ','.join(['?'] * len(event_types))
            query += f" AND event_type IN ({placeholders})"
            params.extend(event_types)
        
        if min_lat is not None:
            query += " AND latitude >= ?"
            params.append(min_lat)
        
        if max_lat is not None:
            query += " AND latitude <= ?"
            params.append(max_lat)
        
        if min_lon is not None:
            query += " AND longitude >= ?"
            params.append(min_lon)
        
        if max_lon is not None:
            query += " AND longitude <= ?"
            params.append(max_lon)
        
        query += " ORDER BY datetime DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        events = []
        for row in rows:
            events.append(dict(row))
        
        self.close()
        return events
    
    def get_events_by_radius(self, center_lat: float, center_lon: float, 
                            radius_km: float, start_date: str = None,
                            end_date: str = None) -> List[Dict]:
        """
        Get events within a radius (approximate using bounding box)
        
        Note: For precise distance calculations, consider using PostGIS or spatialite
        """
        # Approximate conversion: 1 degree ≈ 111 km
        lat_range = radius_km / 111.0
        lon_range = radius_km / (111.0 * abs(cos(radians(center_lat))))
        
        min_lat = center_lat - lat_range
        max_lat = center_lat + lat_range
        min_lon = center_lon - lon_range
        max_lon = center_lon + lon_range
        
        return self.get_events(
            start_date=start_date,
            end_date=end_date,
            min_lat=min_lat,
            max_lat=max_lat,
            min_lon=min_lon,
            max_lon=max_lon
        )
    
    def get_financial_summary(self, start_date: str = None, end_date: str = None) -> Dict:
        """Get financial summary for a date range"""
        conn = self.connect()
        cursor = conn.cursor()
        
        query = '''
        SELECT 
            COUNT(*) as total_transactions,
            SUM(CASE WHEN event_type = 'financial_gain' THEN amount ELSE 0 END) as total_gains,
            SUM(CASE WHEN event_type = 'financial_loss' THEN amount ELSE 0 END) as total_losses,
            AVG(CASE WHEN event_type = 'financial_gain' THEN amount END) as avg_gain,
            AVG(CASE WHEN event_type = 'financial_loss' THEN amount END) as avg_loss
        FROM events
        WHERE event_type IN ('financial_gain', 'financial_loss')
        '''
        
        params = []
        if start_date or end_date:
            if start_date and end_date:
                query += " AND datetime BETWEEN ? AND ?"
                params.extend([start_date, end_date])
            elif start_date:
                query += " AND datetime >= ?"
                params.append(start_date)
            elif end_date:
                query += " AND datetime <= ?"
                params.append(end_date)
        
        cursor.execute(query, params)
        result = dict(cursor.fetchone())
        self.close()
        return result
    
    def export_to_json(self, filename: str = 'events_export.json'):
        """Export all events to JSON file"""
        events = self.get_events(limit=10000)  # Adjust limit as needed
        
        with open(filename, 'w') as f:
            json.dump(events, f, indent=2, default=str)
        
        print(f"Exported {len(events)} events to {filename}")
        return len(events)

# Helper function for cosine calculation
from math import cos, radians