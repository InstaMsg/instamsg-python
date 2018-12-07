import os,sys
from pathlib import Path
import json
# print (Path(__file__).absolute().parent.absolute().parent / 'instamsg')
# sys.path.insert(1, Path(__file__).absolute().parent.absolute().parent / 'instamsg')

from src.instamsg import instamsg
import sys
import time

clientId = ''
authKey = ''

def start(args): 
    try:      
        """
        1. Signup at https://platform.instamsg.io/#/signup
        2. Create an App
        3. Create a Tenant
        4. Create a Client. Use a unique identifier for provisioning id like imei 
           or mac address. Set a provisioning pin.
        5. Note down the provisioning id and pin. Set it in the below parameters.
        """
        
        provId = "12345678"
        provkey = "12345678"
        __startInstaMsg(provId, provkey)

    except:
       print("Unknown Error in start: %s %s" % (str(sys.exc_info()[0]), str(sys.exc_info()[1]))) 

def __startInstaMsg(provId='', provkey=''):
    options = {'logLevel':instamsg.INSTAMSG_LOG_LEVEL_DEBUG, 'enableSsl':1, "connectivity": "wlan0", 'manufacturer':'Sony', 'model':'15CNB'}
    # Try to get auth info from auth.json if file exists
    try:
        auth = __getAuthJson()
        clientId = auth['client_id']
        authKey = auth['auth_token']
        enable_client_side_ssl_certificate = auth['auth_token']
        client_ssl_certificate = auth['certificate']
        client_ssl_certificate_key = auth['key']
        instaMsg = instamsg.InstaMsg(clientId, authKey, __onConnect, __onDisConnect, __oneToOneMessageHandler, options)
        instaMsg.start()            
    except IOError:
        print("File auth.json not found or path is incorrect. Trying provisioning...")
        instamsg.InstaMsg.provision(provId, provkey, __provisionHandler)

def __getAuthJson():
    print("Trying to read auth info from auth.json ...")
    filename =  Path(__file__).absolute().parent / 'auth.json'
    with open(filename,"r") as f:
        auth = json.load(f)
    f.close() 
    return auth   


def __provisionHandler(provMsg):
    """
    1. On succesfull provisioning a file auth.json would be created in 
       current working directory.
    2. These credentials would be used to connect to InstaMsg cloud.
    3. If you loose these credentials you will have to login into your InstamSg
       account and reprovision the client with new provisioning pin.
    """
    print(" Received provisioning response %s . Saving to file auth.json" % provMsg) 
    try:
        filename =  Path(__file__).absolute().parent / 'auth.json'
        with open(filename,"w+") as f:
            json.dump(provMsg ,f)
        f.close()
        __startInstaMsg()
    except IOError:
        print("File not found or path is incorrect")


def __onConnect(instaMsg):
    topic = "subtopic1"
    qos = 0
    __publishMessage(instaMsg, "instamsg/webhook", "Test message 1",1, 0)
    time.sleep(1)
    __publishMessage(instaMsg, "instamsg/webhook", "Test message 2",1, 0)
    time.sleep(1)
    __publishMessage(instaMsg, "instamsg/webhook", "Test message 3",1, 0)
    time.sleep(1)
    __subscribe(instaMsg, topic, qos)
    try:
        """
        messages are loop backed if send to clientId topic
        """

        print("Sending loopbak messages to self...")
        auth = __getAuthJson()
        clientId = auth['client_id']
        authKey = auth['auth_token']
        __publishMessage(instaMsg, clientId, "Test message 4",2, 0)
        time.sleep(1)
        __sendMessage(instaMsg, clientId)
        time.sleep(1)
        __publishMessage(instaMsg, clientId, "Test message 6",0, 0)
    except IOError:
        print("File auth.json not found or path is incorrect. Unable to send loopbak messages to self...")
    time.sleep(10)
    __unsubscribe(instaMsg, topic)

    
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
        instaMsg.publish(topic, msg, qos, dup, resultHandler=_resultHandler)
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
        
def __sendMessage(instaMsg, clientId):
    try:
        msg = "This is a test loopback send message."
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
        instaMsg.send(clientId, msg, qos, dup, _replyHandler, 120)    
    except Exception as e:
        print (str(e))
    
if  __name__ == "__main__":
    rc = start(sys.argv)
    sys.exit(rc)
