#
# Author: L. Saetta
# august 2017
#
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

# open the input file and then... read, pulish loop
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
        del msgJson["raceId"]
        del msgJson["raceStatus"]
        del msgJson["dateTime"]

        if (tipoMsg == "data") and ("lapTime" not in msgJson):
            # speed msgs
            gateway.publish("speed/msg", msg)
            print(msgJson)
        elif tipoMsg == "alert":
             # offtrack msgs
             gateway.publish("alert/msg", msg)           
             print(msgJson)
        elif (tipoMsg == "data") and ("lapTime" in msgJson):
             # lap msgs
             gateway.publish("lap/msg", msg)
             print(msgJson)

        # sleep before next iteration
        nMsgs = nMsgs + 1
        time.sleep(sleepTime)
     
# end main loop

print()
print("*******************")
print("End of simulation...")
print("Num. of messages processed:", nMsgs)

# close
