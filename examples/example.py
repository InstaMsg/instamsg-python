import instamsg as mqtt
import sys
import time
def start(args):
    try:
        try:
            options={}
            options['hasUserName'] = 1
            options['hasPassword'] = 1
            options['debug'] = 1
            options['username'] = "a41f726775dd"
            options['password'] = "AVE5DgIGycSjoiER8k33sIQdPYbJqEe3u"
            options['willTopic'] = "test"
            options['willMessage'] ="test"
            options['isWillRetain'] =0
            options['willQos'] =0
            options['isWillFlag'] =1
            clientId ="62513710-86c0-11e4-9dcf"
            host = "localhost"
            port =1883
            mqttClient = mqtt.MqttClient(host, port, clientId, options)
            mqttClient.onDebugMessage(__debugMessages)
            mqttClient.onConnect(__onConnect)
            mqttClient.onMessage(_messageHandler)
            mqttClient.connect()
            while 1:
                mqttClient.process()
                time.sleep(1)
        except:
            print("Gateway:: Unknown Error in start: %s %s" %(str(sys.exc_info()[0]),str(sys.exc_info()[1])))
    finally:
        pass
    
def __onConnect(mqttClient):
    print "Connected"
    topic = "62513710-86c0-11e4-9dcf-a41f726775dd"
    qos = 0
    __subscribe(mqttClient, topic, qos)
    
def __debugMessages(msg):
    print msg
    
def __subscribe(mqttClient, topic, qos):
    def _unSubscriberesultHandler(messageId):
        print "unSubscribed to topic %s with qos %d" %(topic,qos)
    def _resultHandler(messageId):
        print "Subscribed to topic %s with qos %d" %(topic,0)
        mqttClient.publish("92b58550-86c0-11e4-9dcf-a41f726775dd", "cccccccccc",2)
    mqttClient.subscribe(topic, qos, _resultHandler)
        
def _messageHandler(mqttMessage):
        print "Received message %s" %str(mqttMessage)
    
    
if  __name__ == "__main__":
    rc = start(sys.argv)
    sys.exit(rc)