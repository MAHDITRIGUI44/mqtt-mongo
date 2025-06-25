import json
import time
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime

# MongoDB Atlas setup
MONGO_URI = "mongodb+srv://espuser:esp1234@cluster0.xxxxxx.mongodb.net/solar_project?retryWrites=true&w=majority"
mongo_client = MongoClient(MONGO_URI)
collection = mongo_client["solar_project"]["sensor_data"]

# MQTT setup
MQTT_BROKER = "1488f29f1f3c4c9cb032424a3d60c015.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_TOPIC = "esp32/sensors"
MQTT_USER = "esp32user"
MQTT_PASSWORD = "Esp32pass123"

def on_connect(client, userdata, flags, rc):
    print("✅ Connected to MQTT Broker")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)

        # Add timestamp if not provided
        if "timestamp" not in data:
            data["timestamp"] = datetime.utcnow().isoformat()

        collection.insert_one(data)
        print("📥 Data inserted to MongoDB:", data)
    except Exception as e:
        print("❌ Error:", e)

# Setup MQTT client
client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
client.tls_set()  # SSL enabled
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)

# Loop forever
client.loop_forever()
