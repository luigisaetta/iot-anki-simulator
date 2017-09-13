#!/usr/bin/python

import paho.mqtt.client as mqtt
import json
import datetime
import time
import csv

def on_connect(mqttc, obj, flags, rc):
    global connOk 

    connOk = True
    print("Connection:... rc:", rc)

def on_message(mqttc, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

def on_publish(mqttc, obj, mid):
    # print("mid: " + str(mid))
    pass

def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_log(mqttc, obj, level, string):
    print(string)

#
# Main
#
host = "153.92.32.90"
port = 1883
topic = "speed/msg"
timeout = 60
client = "Saetta"
myQos = 1

demozone = "ROME"
raceStatus = "RACING"
raceCount = 1
deviceId = "0000000051b9c6ae"
myCarName = "Skull"
myDeviceAddress = "myAddr"
trackId = 1
currentLap = 1

# If you want to use a specific client id, use
mqttc = mqtt.Client(client)

# but note that the client id must be unique on the broker. Leaving the client
# id parameter empty will generate a random id for you.
mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish


# Uncomment to enable debug messages
# mqttc.on_log = on_log

mqttc.connect(host, port, timeout)

mqttc.loop_start()

#
i = 1
connOk = False
speed = 10

while True:
    if (connOk):
        dateTimeString = datetime.datetime.now().strftime("%y/%m/%d %H:%M:%S")
        
        # format the JSON msg
        msg = {"demozone": demozone,"deviceId":deviceId,"dateTime":int(time.time()),"dateTimeString":dateTimeString,"raceStatus": raceStatus,"raceId":raceCount,"carId":myDeviceAddress,"carName":myCarName,"speed":speed,"trackId":trackId,"lap":currentLap}
        
        # send msg
        mqttc.publish(topic, json.dumps(msg), qos=myQos)

        # simulate increase
        speed = speed + 5
        
        print("Iteration #", i)
        i = i +1

        if (speed > 90):
            speed = 10

    # wait some time    
    time.sleep(0.1)


