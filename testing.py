import paho.mqtt.client as mqtt
import time

# MQTT broker address and port
broker_address = "10.9.39.25"
broker_port = 1883

# MQTT topic to publish to
topic = "swadha/50KUPS001/Time/log"

# MQTT client initialization
client = mqtt.Client()

# Define callback functions for MQTT events
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker.")
    else:
        print("Failed to connect to MQTT broker. Error code: " + str(rc))

def on_publish(client, userdata, mid):
    print("Published message with MID: " + str(mid))

def on_disconnect(client, userdata, rc):
    print("Disconnected from MQTT broker. Error code: " + str(rc))

# Set MQTT event callbacks
client.on_connect = on_connect
client.on_publish = on_publish
client.on_disconnect = on_disconnect

# Set username and password for MQTT authentication
client.username_pw_set(username="swadha", password="dhawas@123")

# Connect to MQTT broker
client.connect(broker_address, broker_port)

# Start the MQTT client loop
client.loop_start()

# Wait for MQTT connection to be established
while not client.is_connected():
    time.sleep(1)

# Publish the current time in epoch format to the topic
while True:
    current_time = int(time.time()) # Get current time in epoch format
    result, mid = client.publish(topic, str(current_time))
    if result == mqtt.MQTT_ERR_SUCCESS:
        print("Published current time (epoch format):", current_time)
    else:
        print("Failed to publish message. Error code: " + str(result))
    time.sleep(1800)

# Disconnect MQTT client
client.loop_stop()
client.disconnect()

