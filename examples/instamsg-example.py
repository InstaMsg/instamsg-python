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
            instaMsg = instamsg.InstaMsg(clientId, authKey, __onConnect, __onDisConnect, __oneToOneMessageHandler, options)
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
    topic = "subTopic1"
    qos = 0
    __subscribe(instaMsg, topic, qos)
    __publishMessage(instaMsg, "32680660-8098-11e4-94ac-543530e3bc65", "cccccccccccc",2, 0)
    __sendMessage(instaMsg)
    __publishMessage(instaMsg, "32680660-8098-11e4-94ac-543530e3bc65", "bbbbbbbbbbbb",0, 0)
    
def __onDisConnect():
    print "Client disconnected."
    
def __subscribe(instaMsg, topic, qos):
    try:
        def _resultHandler(result):
            print "Subscribed to topic %s with qos %d" %(topic,qos)
    #         print "Unsubscribing from topic %s..." %topic
    #         __unsubscribe(instaMsg, topic)
        instaMsg.subscribe(topic, qos, __messageHandler, _resultHandler)
    except Exception, e:
        print str(e)
    
def __publishMessage(instaMsg, topic, msg, qos, dup):
    try:
        def _resultHandler(result):
            print "Published message %s to topic %s with qos %d" %(msg, topic,qos)
        instaMsg.publish(topic, msg, qos, dup, _resultHandler)
    except Exception, e:
        print str(e)
    
def __unsubscribe(instaMsg, topic):
    try:
        def _resultHandler(result):
            print "UnSubscribed from topic %s" %topic
        instaMsg.unsubscribe(topic, _resultHandler)
    except Exception, e:
        print str(e)
        
def __messageHandler(mqttMessage):
        if(mqttMessage):
            print "Received message %s" %str(mqttMessage.toString())
        
def __oneToOneMessageHandler(msg):
    if(msg):
        print "One to One Message received %s" % msg.toString()
        msg.reply("This is a reply to a one to one message.")
        
def __sendMessage(instaMsg):
    try:
        clienId = "32680660-8098-11e4-94ac-543530e3bc65"
        msg= "This is a test send message."
        qos=1
        dup=0
        def _replyHandler(result):
            if(result.succeeded()):
                replyMessage = result.result()
                if(replyMessage):
                    print "Message received %s" % replyMessage.toString()
                    replyMessage.reply("This is a reply to a reply.")
#                     replyMessage.fail(1, "The message failed")
            else:
                print "Unable to send message errorCode= %d errorMsg=%s" %(result.code[0],result.code[1] )
        instaMsg.send(clienId, msg, qos, dup, _replyHandler, 120)    
    except Exception, e:
        print str(e)
    
if  __name__ == "__main__":
    rc = start(sys.argv)
    sys.exit(rc)