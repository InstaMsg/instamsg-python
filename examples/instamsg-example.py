import instamsg
import sys
import time

def start(args):
    instaMsg = None
    try:
        try:
            options={}
            clientId ="d06f5d10-8091-11e4-bd82-543530e3bc65"
            authKey = "afdlkjghfdjglkjo-094-09k"
            instaMsg = instamsg.InstaMsg(clientId, authKey, __onConnect, __onDisConnect, options)
            while 1:
                instaMsg.process()
                time.sleep(1)
        except:
            print("Unknown Error in start: %s %s" %(str(sys.exc_info()[0]),str(sys.exc_info()[1])))
    finally:
        if(instaMsg):
            instaMsg.close()
            instaMsg = None
    
def __onConnect(instaMsg):
    print "Client connected to InstaMsg IOT cloud service."
#     topic = "62513710-86c0-11e4-9dcf-a41f726775dd"
    topic = "d06f5d10-8091-11e4-bd82-543530e3bc65"
    qos = 0
    __subscribe(instaMsg, topic, qos)
    __publishMessage(instaMsg, "32680660-8098-11e4-94ac-543530e3bc65", "cccccccccc",2, 0)
    
def __onDisConnect():
    print "Client disconnected."
    
def __subscribe(instaMsg, topic, qos):
    def _resultHandler(messageId):
        print "Subscribed to topic %s with qos %d" %(topic,qos)
#         print "Unsubscribing from topic %s..." %topic
#         __unsubscribe(instaMsg, topic)
    instaMsg.subscribe(topic, qos, __messageHandler, _resultHandler)
    
def __publishMessage(instaMsg, topic, msg, qos, dup):
    def _resultHandler(messageId):
        print "Published message %s to topic %s with qos %d" %(msg, topic,qos)
    instaMsg.publish(topic, msg, qos, dup, _resultHandler)
    
def __unsubscribe(instaMsg, topic):
    def _resultHandler(messageId):
        print "UnSubscribed from topic %s" %topic
    instaMsg.unsubscribe(topic, _resultHandler)
        
def __messageHandler(mqttMessage):
        print "Received message %s" %str(mqttMessage.toString())
    
if  __name__ == "__main__":
    rc = start(sys.argv)
    sys.exit(rc)