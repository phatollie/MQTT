# -*- coding: utf-8 -*-
###############################################
# Authored by Justin Acevedo in the year 2021 #
###############################################

"""

Description: MQTT client script to help reduce some confusion on getting connected to a MQTT broker and subscribing to TOPICS/MESSAGES. Optional console verbose logging has been put in place just to give immediate feedback that communications are occuring. Because MQTT is so light weight once you understand how to communicate and publish the verbose console logging is not really needed. Better option is to log to $SYSLOG or client specific file. This script represents the PUBLISH methods separated from the SUBSCIBE method to keeps things simple. Please refer to the MQTT specifications for on best methods to PUBLISH topics and messages. Most hardware/software vendors who utilize MQTT provide what specific topics/messages are required. Network diversity/robustness may require you to tweak settings related to MQTT QOS and time.sleep to ensure topics/messages reach their destination. However, if the script is too overzealous you might break the pipe and drop packets. Topology really plays a part when you expand on PUBLISH volume. Broker redundancy is key when working with larger/bigger networks.

Design: A more pythonic approach would be to separate out GLOBALS / LOGGING into a config module. But for the purposes for this project is for reference/education hence the simple script layout. Note that the order below for connect / publish allows for the MQTT broker to reply timely so callbacks work. Publishing is really "real-time" fast and it's the callbacks themselves that might be slow so don't worry about console logging timeliness. In general, mqtt_pub.py is more on the interactive side as opposed to mqtt_sub.py which sits and loops for new messages. MQTT_pub.py should be run after MQTT_Sub.py is running. Obviously, both require a BROKER to be up and running.    
"""

import paho.mqtt.client as paho
import logging
import time

LOG_FORMAT = '%(levelname)s: %(module)s: %(asctime)s - %(message)s'
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format = LOG_FORMAT)
logging.getLogger()

MQTT_BROKER_HOST = 'localhost'
MQTT_BROKER_PORT = 1883
MQTT_KEEP_ALIVE_INTERVAL = 60

# Dict of devices by room should be separted out to a config file for pythonic cleanliness. 
devices = {
    "room1": [
        "device1",
        "device2",
        "device3",
        "device4",
        "device5"
    ],
    "room2": [
        "device1",
        "device2",
        "device3",
        "device4"
    ]
}

# create a new subscribe list for list provides topic format
topics = list()
for room, devices in devices.items():
    for device in devices:
        topics.append(room + "/" + device)

def on_message(client, userdata, message):
    """
    `on_message` called when a message has been received on a topic that the client subscribes to
    
    :param `client` current Client instance that is calling the callback.  
    :param `userdata` user data of any type and can be set when creating a new client
    instance or with user_data_set(userdata).
    :param `message` message variable is a MQTTMessage that describes all of the message parameters.  
    """ 
    print("Topic: " + message.topic + " Message: " + message.payload.decode('utf-8'))
   

client = paho.Client()

print(f'Establishing a connection to the MQTT Broker...{MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}')
client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_KEEP_ALIVE_INTERVAL)
logger.debug("Broker info from GLOBALS: %s:%s", MQTT_BROKER_HOST, MQTT_BROKER_PORT)

client.on_message = on_message # presents topics/message verbosely on this sub client

print("Subscribing to our test topics...")
client.subscribe("westside/#", qos=1)
time.sleep(2)

print("Subscribing from room list...") # different method to subscribe to topics
for topic in topics:
    client.subscribe(topic) # QoS will default to 0 if not set
    logger.info(f"Room list topics subscribed: {topics}")

print("Waiting for new topics/messages to arrive...")
client.loop_forever()