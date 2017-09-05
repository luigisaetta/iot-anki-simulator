#
# Author: L. Saetta
# august 2017
#
import csv
import json
import time
import sys
import paho.mqtt.client as mqtt


# Custom class for MQTT communication
from Device import Device

#
# functions definition
#
def on_connect(mqttc, obj, flags, rc):
    global connOk
    
    connStatus = "KO"

    if (rc == 0):
        connOk = True
        connStatus = "OK"

    print("")
    print("MQTT Connection:...: ", connStatus)
    print("")

def on_message(mqttc, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

def on_publish(mqttc, obj, mid):
    # print("mid: " + str(mid))
    pass

def on_log(mqttc, obj, level, string):
    print(string)

#
# Configuration for MQTT protocol
#
# the broker, in my Ravello env
host = "153.92.32.90"
port = 1883
# topic name for speed msg
topic = "speed/msg"
# topic name for alert msg
topicAlert ="alert/msg"
timeout = 60
client = "Saetta"
# guaranteed delivery (at least once)
myQos = 1

#
# Other Configurations
#
# time between msg send
sleepTime = 0.2

# the name of the file with data
# fName = "raceData_short.log" 

#
# Main
#

print("*******************")
print("Starting simulation....")
#
# reading file name from command line
#
fName = sys.argv[1]
print("")
print("File name: ", fName)

# set MQTT client ID
mqttc = mqtt.Client(client)
# note that the client id must be unique on the broker

# MQTT callbacks registration
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish

# Uncomment to enable debug messages
# mqttc.on_log = on_log

mqttc.connect(host, port, timeout)

mqttc.loop_start()

connOk = False
nMsgs = 1

#
# Test Device Class
# not yet used
#
# myDevice = Device()

# myDevice.publish("Hello")


with open(fName) as csvfile:
    readCSV = csv.reader(csvfile, delimiter=';')
    
    # wait for MQTT connection OK
    # connOk is set by the callback on_connect
    while (connOk != True):
        print("Waiting for MQTT connection...")
        time.sleep(1)

    # read line by line...    
    for row in readCSV:
        tempo = row[0]
        tipoMsg = row[1]
        msg = row[3]
        # convert in JSON
        msgJson = json.loads(msg)
        
        # remove data not useful from JSON
        del msgJson["carId"]
        del msgJson["deviceId"]
        del msgJson["demozone"]
        
        if ((tipoMsg == "data") and ("lapTime" not in msgJson)):
            # speed msgs
        
            # send !
            mqttc.publish(topic, msg, qos=myQos)
            print(msgJson)

        elif (tipoMsg == "alert"):
             # offtrack msgs
             mqttc.publish(topicAlert, msg, qos=myQos)               
             print(msgJson)

        # for now we don't process lap messages
        
        # sleep before next iteration
        nMsgs = nMsgs + 1
        time.sleep(sleepTime)
     
# end main loop

print("*******************")
print("End of simulation...")
print("Num. of msgs processed", nMsgs)
