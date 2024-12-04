import sqlite3
import urllib.parse
import requests
import json
from typing import List, Dict
import time
from dotenv import load_dotenv
import os

load_dotenv('miner.env')

def create_photos_table(db_path: str):
    """Creates the property_photos table if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS property_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            property_id INTEGER,
            photo_url TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (property_id) REFERENCES properties (property_id)
        )
    ''')
    conn.commit()
    conn.close()

def save_photos_to_db(db_path: str, property_id: int, photo_urls: List[str]):
    """Saves photo URLs to the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for url in photo_urls:
        cursor.execute(
            'INSERT INTO property_photos (property_id, photo_url) VALUES (?, ?)',
            (property_id, url)
        )
    conn.commit()
    conn.close()

def get_property_photos_batch(db_path: str, limit: int = 10) -> Dict[int, List[str]]:
    """Gets property photos from Redfin API and saves them to database."""
    create_photos_table(db_path)
    api_key = os.getenv('RAPIDAPI_KEY')
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
        SELECT address, city, state, zip_code, property_id
        FROM properties 
        WHERE property_id NOT IN (SELECT DISTINCT property_id FROM property_photos)
        LIMIT ?
    """
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print("No new properties found in database")
        return {}
    
    results = {}
    
    for row in rows:
        address, city, state, zip_code, prop_id = row
        print(f"\nProcessing property: {address}, {city}, {state} {zip_code} (ID: {prop_id})")
        
        address = address.strip().replace('#', 'Unit').replace('.', '').replace('  ', ' ').replace(' ', '-')
        city = city.strip().replace(' ', '-')
        
        redfin_url = f"https://www.redfin.com/{state}/{city}/{address}-{zip_code}/home/{prop_id}"
        encoded_redfin_url = urllib.parse.quote(redfin_url, safe='')
        api_url = f"https://redfin-com-data.p.rapidapi.com/property/detail-photos?url={encoded_redfin_url}"
        
        headers = {
            'x-rapidapi-host': 'redfin-com-data.p.rapidapi.com',
            'x-rapidapi-key': api_key
        }
        
        try:
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            photo_urls = []
            if data.get('status') and data.get('data'):
                for photo in data['data']:
                    if 'photoUrls' in photo and 'fullScreenPhotoUrl' in photo['photoUrls']:
                        photo_urls.append(photo['photoUrls']['fullScreenPhotoUrl'])
            
            if photo_urls:
                save_photos_to_db(db_path, prop_id, photo_urls)
            
            results[prop_id] = photo_urls
            print(f"Saved {len(photo_urls)} photos for property {prop_id}")
            
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            print(f"Error making API request for property {prop_id}: {e}")
            results[prop_id] = []
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response for property {prop_id}: {e}")
            results[prop_id] = []
    
    return results

if __name__ == "__main__":
    db_path = "data/miner.db"
    print("Starting photo URL extraction...")
    property_photos = get_property_photos_batch(db_path, 10)