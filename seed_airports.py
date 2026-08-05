import pandas as pd
from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")

def seed_airports():
    client = MongoClient(MONGO_URI)
    db = client["flight_db"]

    if "airports" in db.list_collection_names() and db.airports.count_documents({}) > 0:
        print("[Seeder] Airports collection already exists and is not empty. Skipping seeding.")
        client.close()
        return
        
    print("[Seeder] Updating airport data...")
    url = "https://davidmegginson.github.io/ourairports-data/airports.csv"
    df = pd.read_csv(url)
    
    valid_types = ['small_airport', 'medium_airport', 'large_airport']
    df = df[df['type'].isin(valid_types)]
    
    airports = []
    for _, row in df.iterrows():
        airports.append({
            "icao": str(row['ident']),
            "iata": str(row['iata_code']) if pd.notna(row['iata_code']) else "",
            "name": str(row['name']),
            "lat": float(row['latitude_deg']),
            "lon": float(row['longitude_deg']),
            "type": str(row['type'])
        })
        
    print("[Seeder] Data seeding to MongoDB...")
    db.airports.insert_many(airports)
    db.airports.create_index("icao")
    
    print(f"[Seeder] Success! {len(airports)} airports added to MongoDB.")
    client.close()

if __name__ == "__main__":
    seed_airports()