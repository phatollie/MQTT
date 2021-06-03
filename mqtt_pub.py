# -*- coding: utf-8 -*-
###############################################
# Authored by Justin Acevedo in the year 2021 #
###############################################

"""

Description: MQTT client script to help reduce the massive options to connect to a MQTT broker and publishing TOPICS. The optional console verbose logging has been put in place just to give immediate feedback that communications are occuring. Because MQTT is so light weight once you understand how to communicate and publish the verbose console logging is not really needed. A better option is to log to $SYSLOG or client specific file. This script represents the PUBLISH methods separated from the SUBSCIBE method to keeps things simple. Most hardware/software vendors who utilize MQTT provide what specific topics/messages are required for their sensors. Keep in mind network diversity/robustness may require you to tweak settings related to MQTT QOS and time.sleep to ensure topics/messages reach their destination. However, if the script is too overzealous you might break the pipe and drop packets. Broker redundancy can help when challenged on larger/bigger networks.

Design: Bare bones script to establish a quick broker connect. A better pythonic approach would be to separate out GLOBALS / LOGGING into a config module. Note that the order below for connect / publish allows for the MQTT broker to reply timely so callbacks work. Publishing is really fast and the callbacks themselves might be slow so don't worry about console logging timeliness. PUBLISH is more on the interactive side as opposed to SUBSCRIBE which waits and loops for new messages. MQTT_pub.py should be run after MQTT_Sub.py is running. Both scripts require a BROKER to be up and running. 
"""

import paho.mqtt.client as paho
import time
import logging

LOG_FORMAT = '%(levelname)s: %(module)s: %(asctime)s - %(message)s'
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format = LOG_FORMAT)
logging.getLogger()

MQTT_BROKER_HOST = 'localhost'
MQTT_BROKER_PORT = 1883
MQTT_KEEP_ALIVE_INTERVAL = 60
    
def on_connect(client, userdata, flags, rc):   
    """
    `on_connect` called when the broker responds to a connection request
    
    :param `client` current Client instance that is calling the callback.  
    :param `userdata` user data of any type and can be set when creating a new client
    instance or with user_data_set(userdata).
    :param `flags` dict that contains response flags from the broker.  
    :param `rc` return code determines success/faliure
        0: Connection successful
        1: Connection refused - incorrect protocol version
        2: Connection refused - invalid client identifier
        3: Connection refused - server unavailable
        4: Connection refused - bad username or password
        5: Connection refused - not authorised
        6-255: Currently unused.  
    """ 
    if rc==0:
        print(f"[SUCCESS] Connected OK to MQTT Broker {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT} replied with RESULT CODE = {rc}")
        logger.debug("Broker info: %s:%s", MQTT_BROKER_HOST, MQTT_BROKER_PORT)
    else:
        print(f"[FAILURE] Bad connection to MQTT Broker {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT} replied with RESULT CODE = {rc}")
        logger.debug("Broker info: %s:%s", MQTT_BROKER_HOST, MQTT_BROKER_PORT)
        
def on_publish(client, userdata, mid):
    """
    `on_publish` called when a message that was to be sent using the publish() call has completed transmission to the broker
    
    :param `client` is the current client.  
    :param `userdata` is exactly that user specific data.  
    :param `mid` mid variable used for comparing the mid variable returned for message tracking.  
    """    
    print(f'[SUCCESS] Sent PUBLISH topic/message MID variable returned = {mid}')

client = paho.Client()
client.on_connect = on_connect
client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_KEEP_ALIVE_INTERVAL)
print(f'Establishing a connection to the MQTT Broker...{MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}')
time.sleep(2)
client.loop_start()
client.on_publish = on_publish
time.sleep(2)
logger.info("Publishing our test topics to: %s:%s", MQTT_BROKER_HOST, MQTT_BROKER_PORT)
client.publish("westside/led1", 'DOWN')
time.sleep(1)
client.publish("westside/led2", 'UP')
time.sleep(1)
client.publish("westside/led3", 'DOWN')
time.sleep(1)

client.loop_stop()
client.disconnect() # If you opt to run in -i comment this line so you can manually publish and stay connected. 
