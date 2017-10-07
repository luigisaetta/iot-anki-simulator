#
# Author: L. Saetta
# 27 september 2017
#
# pylint: disable=invalid-name

import csv
import json
import time
import sys
from Device import Device

#
# Other Configurations
#
# time between msg send
sleepTime = 0.2

# the name of the file with data
# fName = "raceData_short.log" 

# This function reorder the fields in the message
def reorderFields(msgJsonIn):
    msgJsonNew = {}
    msgJsonNew['time'] = msgJsonIn['time']
    msgJsonNew['dateTimeString'] = msgJsonIn['dateTimeString']
    msgJsonNew['raceId'] = msgJsonIn['raceId']
    msgJsonNew['carName'] = msgJsonIn['carName']
    msgJsonNew['lap'] = msgJsonIn['lap']
            
    if 'trackId' in msgJsonIn:
        msgJsonNew['trackId'] = msgJsonIn['trackId']
    if 'speed' in msgJsonIn:
        msgJsonNew['speed'] = msgJsonIn['speed']
    if 'lapTime' in msgJsonIn:
        msgJsonNew['lapTime'] = msgJsonIn['lapTime']
    if 'message' in msgJsonIn:
        msgJsonNew['message'] = msgJsonIn['message']
        msgJsonNew['lastKnownTrack'] = msgJsonIn['lastKnownTrack']
    
    return msgJsonNew
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

# MQTT connectivity is encapsulated in Device class
gateway = Device()

gateway.connect()

nMsgs = 1

# open the input file and then... read, publish loop
try:
    with open(fName) as csvfile:
        readerCSV = csv.reader(csvfile, delimiter=';')

        # wait for MQTT connection OK
        # (at this point should be connected)
        gateway.wait_for_conn_ok()

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
            del msgJson["raceStatus"]
            del msgJson["dateTime"]

            # now we have to build the new string to use as MQTT msg (MQTT wants binary)
            msgJson['time'] = tempo

            msgNew = json.dumps(reorderFields(msgJson))

            # different kind of msgs are sent to different topics
            if (tipoMsg == "data") and ("lapTime" not in msgJson):
                # speed msgs
                gateway.publish("speed/msg", msgNew)
            elif tipoMsg == "alert":
                # offtrack msgs
                gateway.publish("alert/msg", msgNew)           
            elif (tipoMsg == "data") and ("lapTime" in msgJson):
                # lap msgs
                gateway.publish("lap/msg", msgNew)
        
            print(reorderFields(msgJson))

            # sleep before next iteration
            nMsgs += 1
            time.sleep(sleepTime)

    # end main loop
except IOError:
    print("Errore: file not found: ", fName)
    print("Interrupted...")
    sys.exit(-1)

print()
print("*******************")
print("End of simulation...")
print("Num. of messages processed:", (nMsgs -1))

# close
