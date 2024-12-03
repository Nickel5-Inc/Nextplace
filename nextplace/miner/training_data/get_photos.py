import sqlite3
import urllib.parse
import requests
import json
from typing import List, Dict
import time

def get_property_photos_batch(db_path: str, api_key: str, limit: int = 10) -> Dict[int, List[str]]:
    """
    Gets property photos from Redfin API.
    
    Args:
        db_path (str): Path to the SQLite database
        api_key (str): Your RapidAPI key
        limit (int): Number of properties to process
        
    Returns:
        Dict[int, List[str]]: Dictionary mapping property_ids to lists of photo URLs
    """
    # First get the properties
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
        SELECT address, city, state, zip_code, property_id
        FROM properties 
        LIMIT ?
    """
    cursor.execute(query, (limit,))
    rows = cursor.fetchall()
    conn.close()
    
    if not rows:
        print("No properties found in database")
        return {}
    
    results = {}
    
    for row in rows:
        address, city, state, zip_code, prop_id = row
        print(f"\nProcessing property: {address}, {city}, {state} {zip_code} (ID: {prop_id})")
        
        # Clean and format the address components
        address = address.strip().replace('#', 'Unit')
        address = address.replace('.', '').replace('  ', ' ')
        address = address.replace(' ', '-')
        city = city.strip().replace(' ', '-')
        
        # Construct the Redfin web URL
        redfin_url = f"https://www.redfin.com/{state}/{city}/{address}-{zip_code}/home/{prop_id}"
        print(f"Constructed Redfin URL: {redfin_url}")
        
        # URL encode the Redfin URL
        encoded_redfin_url = urllib.parse.quote(redfin_url, safe='')
        
        # Construct the API URL
        api_url = f"https://redfin-com-data.p.rapidapi.com/property/detail-photos?url={encoded_redfin_url}"
        
        # Set up the headers
        headers = {
            'x-rapidapi-host': 'redfin-com-data.p.rapidapi.com',
            'x-rapidapi-key': api_key
        }
        
        try:
            # Make the API request
            response = requests.get(api_url, headers=headers)
            print(f"Response status code: {response.status_code}")
            
            response.raise_for_status()
            
            # Parse the JSON response
            data = response.json()
            
            # Extract all full screen photo URLs
            photo_urls = []
            if data.get('status') and data.get('data'):
                for photo in data['data']:
                    if 'photoUrls' in photo and 'fullScreenPhotoUrl' in photo['photoUrls']:
                        photo_urls.append(photo['photoUrls']['fullScreenPhotoUrl'])
            
            results[prop_id] = photo_urls
            print(f"Found {len(photo_urls)} photos for property {prop_id}")
            
            # Add a small delay to avoid rate limiting
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            print(f"Error making API request for property {prop_id}: {e}")
            results[prop_id] = []
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response for property {prop_id}: {e}")
            results[prop_id] = []
    
    return results

# Example usage
if __name__ == "__main__":
    db_path = "data/miner.db"
    api_key = "YOUR_API_KEY"
    
    print("Starting photo URL extraction for first 10 properties...")
    property_photos = get_property_photos_batch(db_path, api_key, 10)
    
    print("\nSummary of results:")
    for prop_id, urls in property_photos.items():
        print(f"\nProperty ID {prop_id}:")
        if urls:
            print(f"Found {len(urls)} photos:")
            for i, url in enumerate(urls, 1):
                print(f"  {i}. {url}")
        else:
            print("No photos found")
