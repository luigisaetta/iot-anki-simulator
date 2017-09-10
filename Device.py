#
# Class providing methods to interact with MQTT broker
# L.S. 2017
#
import time
import configparser
import paho.mqtt.client as mqtt


#
# Configuration for MQTT protocol
# is written in gateway.ini file !
# host is the broker, in my Ravello env

config = configparser.ConfigParser()
config.read('gateway.ini')

# host = "153.92.32.90"
host = config['DEFAULT']['host']
port = int(config['DEFAULT']['port'])
timeout = int(config['DEFAULT']['timeout'])
client = config['DEFAULT']['client']
myQos = int(config['DEFAULT']['myQos'])

topic = config['DEFAULT']['topic']
topicAlert = config['DEFAULT']['topicAlert']

class Device(object):
    """ Classe che incapsula il comportamento di un Device """
    def __init__(self):
        self.connOK = False

        # Create MQTT client and set MQTT client ID
        self.mqttClient = mqtt.Client(client, protocol=mqtt.MQTTv311)
        # note that the client id must be unique on the broker

        # MQTT callbacks registration
        self.mqttClient.on_message = self.on_message
        self.mqttClient.on_connect = self.on_connect
        self.mqttClient.on_publish = self.on_publish
        
        
    # MQTT callbacks definition
    
    def on_connect(self, mqttc, obj, flags, connResult):

        if connResult == 0:
            self.connOK = True

        print("")
        print("MQTT Connection:...: ", self.connOK)
        print("")

    def on_message(self, mqttc, obj, msg):
        print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

    def on_publish(self, mqttc, obj, mid):
        # print("mid: " + str(mid))
        pass

    # end MQTT callbacks definition

    #
    # Public Methods
    #
    def connect(self):
        self.mqttClient.connect(host, port, timeout)

        self.mqttClient.loop_start()

    def wait_for_conn_ok(self):
        while self.connOK != True:
            print("Waiting for MQTT connection...")
            time.sleep(1)

    def publish(self, theTopic, msg):
       print("message published ", msg)
       self.mqttClient.publish(theTopic, msg, qos=myQos)
    

