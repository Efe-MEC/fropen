import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import pandas as pd
import schedule

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
ARCHIVE_BASE_DIR = "/app/archive"

def archive_yesterday_data():
    now_utc = datetime.now(timezone.utc)
    yesterday = now_utc - timedelta(days=1)
    
    start_of_yesterday = datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=timezone.utc)
    start_of_today = start_of_yesterday + timedelta(days=1)
    
    year_str = f"{start_of_yesterday.year}"
    month_str = f"{start_of_yesterday.month}"
    day_str = f"{start_of_yesterday.day}"
    
    partition_dir = os.path.join(ARCHIVE_BASE_DIR, f"year={year_str}", f"month={month_str}", f"day={day_str}")
    
    if os.path.exists(partition_dir) and any(f.endswith(".parquet") for f in os.listdir(partition_dir)):
        print(f"[{datetime.now()}] Data for {year_str}-{month_str}-{day_str} already exists. Skipping...")
        return

    print(f"[{datetime.now()}] Starting archiving for {year_str}-{month_str}-{day_str}")
    
    client = MongoClient(MONGO_URI)
    db = client["flight_db"]
    
    start_ts = start_of_yesterday.timestamp()
    end_ts = start_of_today.timestamp()
    
    query = {"timestamp": {"$gte": start_ts, "$lt": end_ts}}
    cursor = db.raw_flights.find(query, {"_id": 0})
    
    data = list(cursor)
    
    if not data:
        print(f"[{datetime.now()}] no data found for {year_str}-{month_str}-{day_str}.")
        client.close()
        return
        
    os.makedirs(partition_dir, exist_ok=True)
    
    file_uuid = str(uuid.uuid4())
    file_name = f"part-00000-{file_uuid}.c000.snappy.parquet"
    file_path = os.path.join(partition_dir, file_name)

    df = pd.DataFrame(data)
    df.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)
    print(f"[{datetime.now()}] Success! {len(data)} records saved to {file_path}.")
    
    client.close()

def job():
    archive_yesterday_data()

if __name__ == "__main__":
    print("Archiver started.")
    archive_yesterday_data()
    schedule.every().day.at("03:05").do(job)
    
    print("Timer setted.")
    while True:
        schedule.run_pending()
        time.sleep(60)