from Device import Device
import time

# main

gateway = Device()

gateway.connect()

gateway.wait_for_conn_ok()

while True:
    gateway.publish("MQTT msgs...")
    time.sleep(5)