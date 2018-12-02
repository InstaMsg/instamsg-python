from instamsg import instamsg
import sys
import time

def start(args):
    instaMsg = None
    
    try:
        options = {'logLevel':instamsg.INSTAMSG_LOG_LEVEL_DEBUG, 'enableSsl':1, "connectivity": "wlan0", 'manufacturer':'Sony', 'model':'15CNB'}
#         options={'logLevel':instamsg.INSTAMSG_LOG_LEVEL_DEBUG }
        clientId = "33691c30-c567-11e8-814b-bc764e106405"
        authKey = "2932e801a0a64ab4abaa3c2e923f9d19"
        instaMsg = instamsg.InstaMsg(clientId, authKey, __onConnect, __onDisConnect, __oneToOneMessageHandler, options)
        instaMsg.start()
    except:
       print("Unknown Error in start: %s %s" % (str(sys.exc_info()[0]), str(sys.exc_info()[1])))
    
def __onConnect(instaMsg):
    topic = "subtopic1"
    qos = 0
#     __subscribe(instaMsg, topic, qos)
#     __publishMessage(instaMsg, "92b58550-86c0-11e4-9dcf-a41f726775dd5", "cccccccccccc",2, 0)
#     __sendMessage(instaMsg)
#     __publishMessage(instaMsg, "92b58550-86c0-11e4-9dcf-a41f726775dd", "bbbbbbbbbbbb",0, 0)
#     __unsubscribe(instaMsg, topic)
    print ("publish message")
    __publishMessage(instaMsg, "instamsg/webhook", "SenseGrow_akash1",1, 0)
    time.sleep(60)
    __publishMessage(instaMsg, "instamsg/webhook", "SenseGrow_akash2",1, 0)
    time.sleep(60)

    __publishMessage(instaMsg, "instamsg/webhook", "SenseGrow_akash3",1, 0)
    time.sleep(60)

    __publishMessage(instaMsg, "instamsg/webhook", "SenseGrow_akash4",1, 0)
    time.sleep(60)
 
    __publishMessage(instaMsg, "instamsg/webhook", "SenseGrow_akash5",1, 0)
    time.sleep(60)
    
def __onDisConnect():
    print ("Client disconnected.")
    
def __subscribe(instaMsg, topic, qos):
    try:
        def _resultHandler(result):
            print ("Subscribed to topic %s with qos %d" % (topic, qos))
        instaMsg.subscribe(topic, qos, __messageHandler, _resultHandler)
    except Exception as e:
        print (str(e))
    
def __publishMessage(instaMsg, topic, msg, qos, dup):
    try:
        def _resultHandler(result):
            print (result)
            print ("Published message %s to topic %s with qos %d" % (msg, topic, qos))
        instaMsg.publish(topic, msg, qos, dup, _resultHandler)
    except Exception as e:
        print (str(e))
    
def __unsubscribe(instaMsg, topic):
    try:
        def _resultHandler(result):
            print ("UnSubscribed from topic %s" % topic)
        instaMsg.unsubscribe(topic, _resultHandler)
    except Exception as e:
        print (str(e))
        
def __messageHandler(mqttMessage):
        if(mqttMessage):
            print ("Received message %s" % str(mqttMessage.toString()))
        
def __oneToOneMessageHandler(msg):
    if(msg):
        print ("One to One Message received %s" % msg.toString())
        msg.reply("This is a reply to a one to one message.")
        
def __sendMessage(instaMsg):
    try:
        clienId = "92b58550-86c0-11e4-9dcf-a41f726775dd"
        msg = "This is a test send message."
        qos = 1
        dup = 0
        def _replyHandler(result):
            if(result.succeeded()):
                replyMessage = result.result()
                if(replyMessage):
                    print ("Message received %s" % replyMessage.toString())
                    replyMessage.reply("This is a reply to a reply.")
            else:
                print ("Unable to send message errorCode= %d errorMsg=%s" % (result.code[0], result.code[1]))
        instaMsg.send(clienId, msg, qos, dup, _replyHandler, 120)    
    except Exception as e:
        print (str(e))
    
if  __name__ == "__main__":
    rc = start(sys.argv)
    sys.exit(rc)