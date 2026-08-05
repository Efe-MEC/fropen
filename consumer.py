import os
import json
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient, ASCENDING

MONGO_URI = os.getenv("MONGO_URI")
TOPIC_NAME = "flight-data"
BOOTSTRAP_SERVERS = ["kafka1:29092", "kafka2:29092", "kafka3:29092"]

SESSION_TIMEOUT_MINUTES = 30

def setup_database(db):
    existing_collections = db.list_collection_names()

    if "raw_flights" not in existing_collections:
        db.create_collection("raw_flights")
    raw_collection = db["raw_flights"]
    
    if "flight_sessions" not in existing_collections:
        db.create_collection("flight_sessions")
    session_collection = db["flight_sessions"]

    session_collection.create_index([("icao24", ASCENDING), ("is_active", ASCENDING)])

    raw_collection.create_index([("created_at", ASCENDING)], expireAfterSeconds=172800)
    session_collection.create_index([("last_seen", ASCENDING)], expireAfterSeconds=172800)

    return raw_collection, session_collection

def main():
    client = MongoClient(MONGO_URI)
    db = client["flight_db"]

    raw_collection, session_collection = setup_database(db)

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        group_id="flight-consumer-group"
    )

    partitions = consumer.partitions_for_topic(TOPIC_NAME)
    if partitions:
        topic_partitions = [TopicPartition(TOPIC_NAME, p) for p in partitions]
        consumer.assign(topic_partitions)
        timestamp_ms = int((time.time() - 60) * 1000)
        timestamps = {tp: timestamp_ms for tp in topic_partitions}
        offsets = consumer.offsets_for_times(timestamps)
        for tp, offset_and_timestamp in offsets.items():
            if offset_and_timestamp:
                consumer.seek(tp, offset_and_timestamp.offset)
            else:
                consumer.seek_to_end(tp)
    else:
        consumer.subscribe([TOPIC_NAME])

    processed_since_commit = 0
    COMMIT_BATCH_SIZE = 50

    try:
        for message in consumer:
            try:
                flight = json.loads(message.value.decode("utf-8"))
                icao24 = flight.get("icao24")
                unix_ts = flight.get("timestamp")
                dt = datetime.fromtimestamp(unix_ts, tz=timezone.utc) if unix_ts else datetime.now(timezone.utc)
                
                flight_to_insert = flight.copy()
                flight_to_insert["created_at"] = dt
                raw_collection.insert_one(flight_to_insert)

                lat = flight.get("latitude")
                lon = flight.get("longitude")
                alt = flight.get("altitude", 0.0) or 0.0
                on_ground = flight.get("on_ground", False)

                if lat is None or lon is None:
                    continue

                active_session = session_collection.find_one({
                    "icao24": icao24,
                    "is_active": True
                })

                point_data = [float(lat), float(lon), float(alt)]

                if active_session:
                    last_seen = active_session.get("last_seen").replace(tzinfo=timezone.utc)
                    time_diff = dt - last_seen

                    if time_diff > timedelta(minutes=SESSION_TIMEOUT_MINUTES) or on_ground:
                        session_collection.update_one(
                            {"_id": active_session["_id"]},
                            {"$set": {"is_active": False, "last_seen": dt, "end_time": dt}}
                        )
                    else:
                        session_collection.update_one(
                            {"_id": active_session["_id"]},
                            {
                                "$push": {"path": point_data},
                                "$set": {"last_seen": dt, "latest_telemetry": flight}
                            }
                        )
                else:
                    if not on_ground:
                        session_id = f"{icao24}_{dt.strftime('%Y%m%d_%H%M%S')}"
                        new_session = {
                            "_id": session_id,
                            "icao24": icao24,
                            "callsign": flight.get("callsign", "N/A"),
                            "is_active": True,
                            "start_time": dt,
                            "last_seen": dt,
                            "path": [point_data],
                            "latest_telemetry": flight
                        }
                        session_collection.insert_one(new_session)

            except Exception:
                continue

            processed_since_commit += 1
            if processed_since_commit >= COMMIT_BATCH_SIZE:
                consumer.commit()
                processed_since_commit = 0

    except KeyboardInterrupt:
        pass
    finally:
        if processed_since_commit > 0:
            consumer.commit()
        client.close()

if __name__ == "__main__":
    main()