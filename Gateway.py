import time
from Device import Device


# main

gateway = Device()

gateway.connect()

gateway.wait_for_conn_ok()

while True:
    gateway.publish("test/msg", "MQTT msgs...")
    time.sleep(5)