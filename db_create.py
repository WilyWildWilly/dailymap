import sqlite3
import os
from datetime import datetime

def create_database(db_path='world_events.db'):
    """Create and initialize the events database"""
    
    # Connect to SQLite database (creates if doesn't exist)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create events table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT NOT NULL CHECK(event_type IN ('death', 'natural_disaster', 'financial_loss', 'financial_gain')),
        title TEXT,
        description TEXT,
        datetime TEXT NOT NULL,
        latitude REAL NOT NULL CHECK(latitude >= -90 AND latitude <= 90),
        longitude REAL NOT NULL CHECK(longitude >= -180 AND longitude <= 180),
        amount REAL,
        currency TEXT DEFAULT 'USD',
        severity TEXT CHECK(severity IN ('low', 'medium', 'high', 'critical')),
        category TEXT,
        source TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create indexes for faster queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_type ON events(event_type)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_datetime ON events(datetime)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_coordinates ON events(latitude, longitude)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON events(created_at)')
    
    # Create views for easier querying
    cursor.execute('''
    CREATE VIEW IF NOT EXISTS vw_financial_events AS
    SELECT id, event_type, title, datetime, latitude, longitude, amount, currency
    FROM events
    WHERE event_type IN ('financial_loss', 'financial_gain')
    ''')
    
    cursor.execute('''
    CREATE VIEW IF NOT EXISTS vw_world_events AS
    SELECT id, event_type, title, datetime, latitude, longitude, severity
    FROM events
    WHERE event_type IN ('death', 'natural_disaster')
    ''')
    
    # Insert some sample data
    sample_events = [
        ('death', 'Earthquake Casualties', 'Major earthquake casualties',
         '2024-01-01 08:30:00', 34.0522, -118.2437, None, 'USD', 'high', 'disaster', 'news'),
        
        ('natural_disaster', 'Hurricane Delta', 'Category 4 hurricane',
         '2024-01-15 14:00:00', 25.7617, -80.1918, None, 'USD', 'critical', 'weather', 'NOAA'),
        
        ('financial_gain', 'Stock Market Rally', 'Tech stocks surge',
         '2024-01-20 16:00:00', 40.7128, -74.0060, 1500000.00, 'USD', None, 'stocks', 'market_data'),
        
        ('financial_loss', 'Company Bankruptcy', 'Major retailer files Chapter 11',
         '2024-02-01 09:45:00', 41.8781, -87.6298, -5000000.00, 'USD', 'high', 'bankruptcy', 'financial_news'),
    ]
    
    cursor.executemany('''
    INSERT INTO events (event_type, title, description, datetime, latitude, longitude, amount, currency, severity, category, source)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', sample_events)
    
    conn.commit()
    print(f"Database created successfully at: {os.path.abspath(db_path)}")
    print(f"Total events inserted: {cursor.rowcount}")
    
    # Display table structure
    cursor.execute("PRAGMA table_info(events)")
    columns = cursor.fetchall()
    print("\nTable structure:")
    print("-" * 80)
    for col in columns:
        print(f"{col[1]:20} {col[2]:15} {'NOT NULL' if col[3] else 'NULL':10}")
    
    conn.close()
    return db_path

def test_database(db_path='world_events.db'):
    """Test the database with some queries"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("\n" + "="*80)
    print("DATABASE TEST QUERIES")
    print("="*80)
    
    # Query 1: Count events by type
    print("\n1. Event count by type:")
    cursor.execute('''
    SELECT event_type, COUNT(*) as count 
    FROM events 
    GROUP BY event_type 
    ORDER BY count DESC
    ''')
    for row in cursor.fetchall():
        print(f"  {row[0]:20}: {row[1]} events")
    
    # Query 2: Recent events
    print("\n2. Recent events:")
    cursor.execute('''
    SELECT event_type, title, datetime, latitude, longitude
    FROM events
    ORDER BY datetime DESC
    LIMIT 3
    ''')
    for row in cursor.fetchall():
        print(f"  {row[0]:20} - {row[1]:30} ({row[2][:10]})")
    
    # Query 3: Financial summary
    print("\n3. Financial summary:")
    cursor.execute('''
    SELECT 
        event_type,
        COUNT(*) as transactions,
        SUM(CASE WHEN event_type = 'financial_gain' THEN amount ELSE 0 END) as total_gains,
        SUM(CASE WHEN event_type = 'financial_loss' THEN ABS(amount) ELSE 0 END) as total_losses
    FROM events
    WHERE event_type IN ('financial_gain', 'financial_loss')
    GROUP BY event_type
    ''')
    for row in cursor.fetchall():
        print(f"  {row[0]:20}: {row[1]} transactions, Amount: ${row[2] or row[3]:,.2f}")
    
    conn.close()

if __name__ == "__main__":
    # Create database
    db_path = create_database()
    
    # Test database
    test_database(db_path)