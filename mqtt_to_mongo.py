import os
import json
import time
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime, timezone

# MongoDB Atlas setup
MONGO_URI = os.getenv("MONGO_URI")
mongo_client = MongoClient(MONGO_URI)
collection = mongo_client["solar_project"]["sensor_data"]

# MQTT setup
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT Broker")
        client.subscribe(MQTT_TOPIC)
    else:
        print("❌ Failed to connect, return code:", rc)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        # Add timestamp if not provided
        if "timestamp" not in data:
            data["timestamp"] = datetime.now(timezone.utc).isoformat()

        collection.insert_one(data)
        print("📥 Data inserted to MongoDB:", data)
    except Exception as e:
        print("❌ Error processing message:", e)

# Setup MQTT client with correct version
client = mqtt.Client(callback_api_version=mqtt.MQTTv311)
client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
client.tls_set()  # Enable TLS encryption
client.on_connect = on_connect
client.on_message = on_message

# Connect to MQTT broker and start loop
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_forever()
