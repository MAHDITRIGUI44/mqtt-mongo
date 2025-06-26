import os
import json
import time
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime

# Lire les variables d'environnement
MONGO_URI = os.getenv("MONGO_URI")
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

# MongoDB Atlas
mongo_client = MongoClient(MONGO_URI)
collection = mongo_client["solar_project"]["sensor_data"]

# MQTT setup
def on_connect(client, userdata, flags, rc):
    print("✅ Connected to MQTT Broker")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        if "timestamp" not in data:
            data["timestamp"] = datetime.utcnow().isoformat()

        collection.insert_one(data)
        print("📥 Data inserted to MongoDB:", data)
    except Exception as e:
        print("❌ Error:", e)

client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.loop_forever()
