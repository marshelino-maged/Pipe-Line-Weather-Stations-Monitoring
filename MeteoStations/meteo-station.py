import time
import requests
import json
from kafka import KafkaProducer
import schedule
from datetime import datetime
import os

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BROKER"),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

stations = [
    {"station_id": 11, "latitude": 30.0444, "longitude": 31.2357},  # Cairo
    {"station_id": 22, "latitude": 40.7128, "longitude": -74.0060}, # New York
    {"station_id": 33, "latitude": 51.5074, "longitude": -0.1278},  # London
]

serial_numbers = {station['station_id']: 0 for station in stations}

def fetch_weather(latitude: float, longitude: float) -> dict:
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={latitude}&longitude={longitude}"
        f"&current_weather=true"
        f"&hourly=relative_humidity_2m"
        f"&timezone=UTC"
    )
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Get the last available hourly humidity
    try:
        last_index = len(data["hourly"]["time"]) - 1
        humidity = data["hourly"]["relative_humidity_2m"][last_index]
    except (KeyError, IndexError) as e:
        raise Exception("Humidity data not found or malformed.") from e

    data['current_weather']['humidity'] = humidity
    return data['current_weather']

def get_battery_status() -> str:
    r = int(time.time()) % 100
    if r < 30:
        return "low"
    elif r < 70:
        return "medium"
    return "high"

def transform_weather_data(station_id: int, s_no: int, weather_raw: dict) -> dict:
    return {
        "station_id": station_id,
        "s_no": s_no,
        "battery_status": get_battery_status(),
        "status_timestamp": int(time.time()),
        "weather": {
            "humidity": int(weather_raw['humidity']),
            "temperature": int((weather_raw['temperature'] * 9 / 5) + 32),  # C to F
            "wind_speed": int(weather_raw['windspeed'])
        }
    }

def send_to_kafka(producer: KafkaProducer, topic: str, message: dict):
    print("Sending:", message)
    producer.send(topic, key=message['station_id'], value=message)
    producer.flush()
    print("Sent:", message)

def collect_and_send():
    for station in stations:
        station_id = station['station_id']
        serial_numbers[station_id] += 1
        try:
            raw_weather = fetch_weather(station['latitude'], station['longitude'])
            message = transform_weather_data(station_id, serial_numbers[station_id], raw_weather)
            print(message)
            send_to_kafka(producer, KAFKA_TOPIC, message)
        except Exception as e:
            print(f"Error for station {station_id}:", e)

if __name__ == "__main__":
    collect_and_send()
    schedule.every(1).seconds.do(collect_and_send)

    print("Weather data producer started for multiple stations.")

    while True:
        schedule.run_pending()
        time.sleep(1)
