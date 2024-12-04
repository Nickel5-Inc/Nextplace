import requests
import sqlite3
from datetime import datetime
import json
from typing import List, Dict
import sys

def setup_database(db_path: str = 'data/miner.db'):
    """Setup the miner database with necessary tables"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create properties table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties (
            nextplace_id TEXT,
            property_id INTEGER,
            listing_id INTEGER,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code INTEGER,
            price INTEGER,
            beds INTEGER,
            baths INTEGER,
            sqft INTEGER,
            lot_size INTEGER,
            year_built INTEGER,
            days_on_market INTEGER,
            latitude REAL,
            longitude REAL,
            property_type INTEGER,
            last_sale_date TEXT,
            hoa_dues INTEGER,
            query_date TEXT,
            market TEXT
        )
    """)
    
    # Create index if it doesn't exist
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_property_id ON properties(property_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nextplace_id ON properties(nextplace_id)")
    
    conn.commit()
    conn.close()

def fetch_nextplace_data() -> List[Dict]:
    """Fetch data from NextPlace API"""
    url = "https://dev-nextplace-api.azurewebsites.net/Properties/Current"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        sys.exit(1)

def save_properties(properties: List[Dict], db_path: str = 'data/miner.db'):
    """Save properties to database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Current timestamp for query_date
    current_time = datetime.utcnow().isoformat()
    
    # Prepare data for insertion
    property_data = []
    for prop in properties:
        property_data.append((
            prop.get('nextplaceId'),
            prop.get('propertyId'),
            prop.get('listingId'),
            prop.get('address'),
            prop.get('city'),
            prop.get('state'),
            prop.get('zipCode'),
            prop.get('price'),
            prop.get('beds'),
            prop.get('baths'),
            prop.get('sqft'),
            prop.get('lotSize'),
            prop.get('yearBuilt'),
            prop.get('daysOnMarket'),
            prop.get('latitude'),
            prop.get('longitude'),
            prop.get('propertyType'),
            prop.get('lastSaleDate'),
            prop.get('hoaDues'),
            current_time,
            prop.get('market')
        ))
    
    # Insert data
    cursor.executemany("""
        INSERT INTO properties (
            nextplace_id, property_id, listing_id, address, city, state, 
            zip_code, price, beds, baths, sqft, lot_size, year_built, 
            days_on_market, latitude, longitude, property_type, 
            last_sale_date, hoa_dues, query_date, market
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, property_data)
    
    conn.commit()
    
    # Get counts for reporting
    cursor.execute("SELECT COUNT(*) FROM properties")
    total_count = cursor.fetchone()[0]
    
    conn.close()
    
    return len(property_data), total_count

def main():
    """Main function to fetch and save data"""
    print("Setting up database...")
    setup_database()
    
    print("Fetching data from NextPlace API...")
    properties = fetch_nextplace_data()
    
    print(f"Fetched {len(properties)} properties")
    print("Saving to database...")
    
    inserted_count, total_count = save_properties(properties)
    
    print(f"Successfully saved {inserted_count} new properties")
    print(f"Total properties in database: {total_count}")

if __name__ == "__main__":
    main()
