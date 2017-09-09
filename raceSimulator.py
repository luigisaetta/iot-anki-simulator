#
# Author: L. Saetta
# august 2017
#
import csv
import json
import time
import sys
import paho.mqtt.client as mqtt

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

def on_disconnect():
    print("MQTT connection closed")

def wait_for_conn_ok():
    while (connOk != True):
        print("Waiting for MQTT connection...")
        time.sleep(1)
#
# Configuration for MQTT protocol
#
# the broker, in my Ravello env
host = "153.92.32.90"
port = 1883
# Topics
# topic name for speed msg
topic = "speed/msg"
# topic name for alert msg
topicAlert = "alert/msg"
#topic name for lap msgs
topicLap = "lap/msg"

timeout = 60
client = "Saetta"
# QoS: guaranteed delivery (at least once)
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
# reading file name from command line args
#
fName = sys.argv[1]
print("")
print("File name: ", fName)

# set MQTT client ID
mqttClient = mqtt.Client(client)
# note that the client id must be unique on the broker

# MQTT callbacks registration
mqttClient.on_message = on_message
mqttClient.on_connect = on_connect
mqttClient.on_publish = on_publish
mqttClient.on_disconnect = on_disconnect

# Uncomment to enable debug messages
# mqttc.on_log = on_log

mqttClient.connect(host, port, timeout)

mqttClient.loop_start()

connOk = False
nMsgs = 1

# open the input file and then... read, pulish loop
with open(fName) as csvfile:
    readerCSV = csv.reader(csvfile, delimiter=';')
    
    # wait for MQTT connection OK
    # connOk is set by the callback on_connect
    # (at this point should be connected)
    wait_for_conn_ok()

    # read line by line...    
    for row in readerCSV:
        tempo = row[0]
        tipoMsg = row[1]
        msg = row[3]
        # convert in JSON
        msgJson = json.loads(msg)
        
        # remove data not useful for Console from JSON
        del msgJson["carId"]
        del msgJson["deviceId"]
        del msgJson["demozone"]
        del msgJson["raceId"]
        del msgJson["raceStatus"]
        del msgJson["dateTime"]

        if ((tipoMsg == "data") and ("lapTime" not in msgJson)):
            # speed msgs
            # send !
            mqttClient.publish(topic, msg, qos=myQos)
            print(msgJson)

        elif (tipoMsg == "alert"):
             # offtrack msgs
             mqttClient.publish(topicAlert, msg, qos=myQos)           
             print(msgJson)
        
        elif ((tipoMsg == "data") and ("lapTime" in msgJson)):
             # lap msgs
             mqttClient.publish(topicLap, msg, qos=myQos)
             print("LAPMSG *** ", msgJson)

      
        
        # sleep before next iteration
        nMsgs = nMsgs + 1
        time.sleep(sleepTime)
     
# end main loop

print()
print("*******************")
print("End of simulation...")
print("Num. of msgs processed:", nMsgs)

# close
mqttClient.disconnect()
