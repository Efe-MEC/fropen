import json
import math
import os
import time
import requests
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

TOPIC_NAME = 'flight-data'
BOOTSTRAP_SERVERS = ['kafka1:29092', 'kafka2:29092', 'kafka3:29092']

OPENSKY_URL = 'https://opensky-network.org/api/states/all'
TOKEN_URL = 'https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token'

CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")

access_token = None
token_expires_at = 0
last_known_headings = {}


def normalize_heading(value):
    if value is None:
        return None

    try:
        heading = float(value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(heading):
        return None

    return heading

def create_topic_if_not_exists():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id='flight-admin'
        )
        topic_list = [
            NewTopic(name=TOPIC_NAME, num_partitions=3, replication_factor=3)
        ]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        admin_client.close()
    except TopicAlreadyExistsError:
        print(f"[Producer] Topic '{TOPIC_NAME}' already exists.")
    except Exception as e:
        print(f"[Producer] Topic creation warning/error: {e}")

def get_access_token():
    global access_token, token_expires_at
    
    if access_token and time.time() < token_expires_at - 10:
        return access_token

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    
    try:
        response = requests.post(TOKEN_URL, headers=headers, data=data, timeout=10)
        if response.status_code == 200:
            token_json = response.json()
            access_token = token_json.get('access_token')
            expires_in = token_json.get('expires_in', 300)
            token_expires_at = time.time() + expires_in
            print("[Producer] Access Token successfully retrieved.")
            return access_token
        else:
            print(f"[Producer] Token error (HTTP {response.status_code}): {response.text}")
            return None
    except Exception as e:
        print(f"[Producer] Token request error: {e}")
        return None

def fetch_and_send_flight_data(producer):
    token = get_access_token()
    if not token:
        print("[Producer] No valid access token found, API request skipped.")
        return

    headers = {'Authorization': f'Bearer {token}'}

    try:
        response = requests.get(OPENSKY_URL, headers=headers, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            states = data.get('states', [])
            
            if not states:
                print("[Producer] API warning: Empty data received.")
                return

            print(f"[Producer] Fetched {len(states)} live flight records.")

            for state in states:
                if state[5] is None or state[6] is None:
                    continue

                icao24_code = state[0]
                callsign = state[1].strip() if state[1] else 'N/A'
                current_heading = normalize_heading(state[10])

                if current_heading is not None:
                    last_known_headings[icao24_code] = current_heading

                heading = current_heading if current_heading is not None else last_known_headings.get(icao24_code)

                flight_payload = {
                    'icao24': icao24_code,
                    'callsign': callsign,
                    'origin_country': state[2] or 'N/A',
                    'time_position': state[3],
                    'longitude': state[5],
                    'latitude': state[6],
                    'altitude': state[7],
                    'velocity': state[9],
                    'timestamp': time.time()
                }

                if heading is not None:
                    flight_payload['heading'] = heading
                
                producer.send(TOPIC_NAME, key=icao24_code, value=flight_payload)
            
            producer.flush()
            print(f"[Producer] Flight data sent to Kafka topic '{TOPIC_NAME}'.\n")

        elif response.status_code == 429:
            print("[Producer] Rate limit hit (HTTP 429). Waiting 30s...")
            time.sleep(30)
        else:
            print(f"[Producer] API Error: HTTP {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"[Producer] Request exception: {e}")

if __name__ == '__main__':
    print("[Producer] System initialized.")
    create_topic_if_not_exists()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    while True:
        fetch_and_send_flight_data(producer)
        time.sleep(60)