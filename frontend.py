from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from pymongo import MongoClient
import os
import json
import asyncio
from datetime import datetime, timezone
from aiokafka import AIOKafkaConsumer
from contextlib import asynccontextmanager
import time
from aiokafka import AIOKafkaConsumer, TopicPartition
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import pandas as pd

MONGO_URI = os.getenv("MONGO_URI")
KAFKA_BOOTSTRAP_SERVERS = "kafka1:29092,kafka2:29092,kafka3:29092"
KAFKA_TOPIC = "flight-data"

active_flights = {}
active_connections = set()

async def consume_kafka_background():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest" 
    )

    while True:
        try:
            await consumer.start()
            print("[API] Kafka connected!")
            break
        except Exception as e:
            print(f"[API] Kafka not ready yet")
            await asyncio.sleep(5)

    try:
        await consumer.getmany(timeout_ms=1000)
        
        assigned_partitions = consumer.assignment()
        
        if assigned_partitions:
            timestamp_ms = int((time.time() - 60) * 1000)
            timestamps = {tp: timestamp_ms for tp in assigned_partitions}
            
            offsets = await consumer.offsets_for_times(timestamps)
            
            for tp, offset_and_timestamp in offsets.items():
                if offset_and_timestamp:
                    consumer.seek(tp, offset_and_timestamp.offset)

        async for msg in consumer:
            flight = msg.value
            lat = flight.get("latitude")
            lon = flight.get("longitude")
            icao24 = flight.get("icao24")
            
            if lat is not None and lon is not None and icao24:
                unix_ts = flight.get("timestamp")
                if unix_ts:
                    dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc)
                else:
                    dt = datetime.now(timezone.utc)
                
                time_str = dt.strftime('%H:%M:%S')

                formatted_flight = {
                    "icao24": icao24,
                    "callsign": flight.get("callsign", "N/A"),
                    "latitude": float(lat),
                    "longitude": float(lon),
                    "altitude": float(flight.get("altitude", 0.0) or 0.0),
                    "velocity": float(flight.get("velocity", 0.0) or 0.0),
                    "origin_country": flight.get("origin_country", "N/A"),
                    "last_updated": time_str,
                    "heading": float(flight.get("heading", flight.get("true_track", 0.0)) or 0.0),
                    "on_ground": flight.get("on_ground", False)
                }
                
                active_flights[icao24] = formatted_flight
                
                dead_connections = set()
                for connection in active_connections:
                    try:
                        await connection.send_json(formatted_flight)
                    except:
                        dead_connections.add(connection)
                
                for dead in dead_connections:
                    active_connections.remove(dead)
    finally:
        await consumer.stop()

def _process_single_parquet(file_path, icao24):
    try:
        df = pd.read_parquet(file_path)
        if "icao24" in df.columns:
            filtered_df = df[df["icao24"] == icao24]
            if not filtered_df.empty:
                return filtered_df[["timestamp", "latitude", "longitude", "altitude"]].to_dict('records')
    except Exception as e:
        print(f"[API] Error reading Parquet file ({file_path}): {e}")
    return []

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_kafka_background())
    yield
    task.cancel()

app = FastAPI(title="Flight Tracker API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client["flight_db"]
raw_flights = db["raw_flights"]
session_flights = db["flight_sessions"]

@app.get("/", response_class=HTMLResponse)
def read_root():
    html_file_path = os.path.join(os.path.dirname(__file__), "frontend.html")
    if os.path.exists(html_file_path):
        with open(html_file_path, "r", encoding="utf-8") as f:
            return f.read()
    return "<h1>frontend.html file not found!</h1>"

@app.get("/style.css")
def read_css():
    file_path = os.path.join(os.path.dirname(__file__), "style.css")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return HTMLResponse("Not found", status_code=404)

@app.get("/script.js")
def read_js():
    file_path = os.path.join(os.path.dirname(__file__), "script.js")
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return HTMLResponse("Not found", status_code=404)

@app.get("/api/health")
def health():
    try:
        client.admin.command("ping")
        return {"status": "ok", "mongodb": "connected"}
    except Exception as e:
        return {"status": "error", "mongodb": str(e)}

@app.get("/api/flights")
def get_flights():
    flights = list(active_flights.values())
    return {
        "count": len(flights),
        "flights": flights,
    }

@app.get("/api/flights/search")
def search_flights(q: str = Query(..., min_length=1)):
    search_term = q.strip().lower()
    results = []
    
    for flight in active_flights.values():
        icao = flight.get("icao24", "").lower()
        callsign = flight.get("callsign", "").lower()
        
        if search_term in icao or search_term in callsign:
            results.append({
                "icao24": flight.get("icao24", "N/A"),
                "callsign": flight.get("callsign", "N/A"),
                "latitude": float(flight.get("latitude", 0)),
                "longitude": float(flight.get("longitude", 0)),
                "altitude": float(flight.get("altitude", 0) or 0),
                "origin_country": flight.get("origin_country", "N/A")
            })
            
            if len(results) >= 10:
                break
                
    return {"results": results}

@app.get("/api/flights/{icao24}/track")
def get_flight_track(icao24: str):
    session = session_flights.find_one({"icao24": icao24, "is_active": True})
    path = session.get("path", []) if session else []
    
    return {
        "icao24": icao24,
        "path": path
    }

@app.get("/api/flights/{icao24}/history-by-date")
def get_flight_history_by_date(icao24: str, date: str = Query(...)):
    try:
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
    except ValueError:
        return {"icao24": icao24, "path": [], "error": "Invalid date format. Use YYYY-MM-DD"}
    
    today_date = datetime.now(timezone.utc).date()
    path = []

    if target_date == today_date:
        start_of_day = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
        start_ts = start_of_day.timestamp()
        
        cursor = raw_flights.find({"icao24": icao24, "timestamp": {"$gte": start_ts}}).sort("timestamp", 1)
        for doc in cursor:
            lat = doc.get("latitude")
            lon = doc.get("longitude")
            alt = doc.get("altitude", 0.0) or 0.0
            if lat is not None and lon is not None:
                path.append([float(lat), float(lon), float(alt)])
    else:
        partition_candidates = [
            os.path.join(
                "/app/archive",
                f"year={target_date.year}",
                f"month={target_date.month}",
                f"day={target_date.day}",
            ),
            os.path.join(
                "/app/archive",
                f"year={target_date.year}",
                f"month={target_date.strftime('%m')}",
                f"day={target_date.strftime('%d')}",
            ),
        ]

        for partition_dir in partition_candidates:
            print(f"[API] Searching archive: {partition_dir} (Aircraft: {icao24})")

            if not os.path.exists(partition_dir):
                continue

            parquet_files = [os.path.join(partition_dir, f) for f in os.listdir(partition_dir) if f.endswith(".parquet")]

            for file_path in parquet_files:
                try:
                    df = pd.read_parquet(file_path)
                    if "icao24" in df.columns:
                        filtered_df = df[df["icao24"] == icao24]
                        if not filtered_df.empty:
                            filtered_df = filtered_df.sort_values("timestamp")
                            for _, row in filtered_df.iterrows():
                                lat = row.get("latitude")
                                lon = row.get("longitude")
                                alt = row.get("altitude", 0.0) or 0.0
                                if pd.notna(lat) and pd.notna(lon):
                                    path.append([float(lat), float(lon), float(alt) if pd.notna(alt) else 0.0])
                except Exception as e:
                    print(f"[API] Error reading Parquet file ({file_path}): {e}")

            if path:
                break

        if not path:
            print(f"[API] Archive directory not found: {partition_candidates[0]}")

    return {
        "icao24": icao24,
        "date": date,
        "path": path
    }

@app.get("/api/flights/{icao24}/history")
def get_flight_history(icao24: str):
    path_points = []

    cursor = raw_flights.find({"icao24": icao24})
    for doc in cursor:
        lat = doc.get("latitude")
        lon = doc.get("longitude")
        alt = doc.get("altitude", 0.0) or 0.0
        ts = doc.get("timestamp", 0)
        
        if lat is not None and lon is not None:
            path_points.append({
                "timestamp": ts, 
                "lat": float(lat), 
                "lon": float(lon), 
                "alt": float(alt)
            })

    archive_dir = "/app/archive"
    parquet_files = []
    if os.path.exists(archive_dir):
        for root, _, files in os.walk(archive_dir):
            for file in files:
                if file.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, file))
    
    if parquet_files:
        print(f"[API] Searching full history for {icao24} in {len(parquet_files)} parquet files via multiprocessing...")

        worker = partial(_process_single_parquet, icao24=icao24)

        with ProcessPoolExecutor() as executor:
            results = executor.map(worker, parquet_files)
            
            for res in results:
                for row in res:
                    ts = row.get("timestamp", 0)
                    lat = row.get("latitude")
                    lon = row.get("longitude")
                    alt = row.get("altitude", 0.0) or 0.0
                    
                    if pd.notna(lat) and pd.notna(lon):
                        path_points.append({
                            "timestamp": ts, 
                            "lat": float(lat), 
                            "lon": float(lon), 
                            "alt": float(alt) if pd.notna(alt) else 0.0
                        })

    path_points.sort(key=lambda x: x["timestamp"])
    path = [[p["lat"], p["lon"], p["alt"]] for p in path_points]
            
    return {
        "icao24": icao24,
        "path": path,
        "total_points": len(path)
    }

@app.websocket("/ws/flights")
async def websocket_flights(websocket: WebSocket):
    await websocket.accept()
    
    for flight in active_flights.values():
        await websocket.send_json(flight)
        
    active_connections.add(websocket)
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)