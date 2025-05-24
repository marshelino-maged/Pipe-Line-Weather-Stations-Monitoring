import json
import os
import time
import random
from kafka import KafkaProducer

# Template for the weather station data
weather_data = {
    "station_id": 1, # Long
    "s_no": 1, # Long auto-incremental with each message per service
    "battery_status": "low", # String of (low, medium, high)
    "status_timestamp": 1681521224, # Long Unix timestamp
    "weather": {
        "humidity": 35, # Integer percentage
        "temperature": 100, # Integer in fahrenheit
        "wind_speed": 13 # Integer km/h
    }
}

# Randomly values for the weather data
def get_weather_data():
    return {
        "humidity": random.randint(0, 100),
        "temperature": random.randint(0, 120),
        "wind_speed": random.randint(0, 50)
    }

# low (30%), medium (40%), high (30%)
def get_battery_status():
    rand = random.random()
    if rand <= 0.3:
        return "low"
    elif rand <= 0.7:
        return "medium"
    else:
        return "high"

# Drop 10% of the messages
def will_drop():
    rand = random.random()
    if rand <= 0.1:
        return True
    else:
        return False


def send_data_to_kafka(producer, weather_data, topic):
    producer.send(
        topic,
        value=json.dumps(weather_data),
    )
    print(f"producing message to topic {topic}")
    producer.flush()  # Ensure the message is sent
    print("message sent to kafka")


if __name__ == "__main__":

    station_id = os.getenv("STATION_ID")

    if station_id == None:
        raise Exception("STATION_ID not set")
    else:
        weather_data["station_id"] = station_id = int(station_id.split("-")[-1]) + 1
        # print(f"station_id: {station_id}")
    
    KAFKA_BROKER = os.getenv("KAFKA_BROKER")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
    # KAFKA_BROKER = "localhost:9092"
    # KAFKA_TOPIC = "weather_topic"

    # print(f"KAFKA_BROKER: {KAFKA_BROKER}")
    # print(f"KAFKA_TOPIC: {KAFKA_TOPIC}")
    # print("Starting weather station...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    )
    
    status_number = 0
    while(True):

        time.sleep(1)
        status_number += 1

        weather_data["battery_status"] = get_battery_status()
        weather_data["weather"] = get_weather_data()
        weather_data["status_timestamp"] = int(time.time())
        weather_data["s_no"] = status_number

        if will_drop():
            continue

        send_data_to_kafka(producer, weather_data, KAFKA_TOPIC)

        json_message = json.dumps(weather_data)
        print(json_message)

        