import json
import mysql.connector
import paho.mqtt.client as mqtt
import time

# MQTT broker information
broker_address = "10.9.39.25"
broker_port = 1883
mqtt_username = "swadha"
mqtt_password = "dhawas@123"

# Define MQTT topics to publish to
mqtt_topic_publish = "swadha/50KUPS001/ins/log"

# Connect to the database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="22@teneT",
    database="EMS"
)
cursor = conn.cursor()

# Initialize MQTT client
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)
mqtt_client.connect(broker_address, broker_port)

# Loop to periodically fetch and publish new data
while True:
    # Fetch data from the database
    cursor.execute("SELECT functioncode, instanttimestamp, batterystatus FROM instantaneous_ups ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    print(row)

    if row:
        data_dict = {
            "FN": row[0],
            "TS": int(row[1].timestamp())
        }

        json_data = json.dumps(data_dict)
        print(json_data)

        try:
            # Publish data to MQTT topic
            mqtt_client.publish(mqtt_topic_publish, json_data)

        except Exception as e:
            print(f"An error occurred while publishing to MQTT: {e}")
            print("Attempting to reconnect to MQTT broker...")

            # Reconnect to MQTT broker
            mqtt_client.reconnect()
            mqtt_client.publish(mqtt_topic_publish, json_data)

    # Commit changes to database
    conn.commit()

    time.sleep(2)



