import os,sys
import json
import inspect

#add parent directory to path so that modules can be imported when example 
#script directly run from current folder 
#$ python path/to/examples/instamsg-example.py
#or via $make run

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 


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
    options = {
                'logLevel':instamsg.INSTAMSG_LOG_LEVEL_DEBUG, 
                'enableTcp':1,
                'enableSsl':1, 
                "connectivity": "wlan0",
                'configHandler': __configHandler,
                'rebootHandler': __rebootHandler,
                'metadata': __getDeviceMetadata()
                }
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
    currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) 
    filename =  os.path.join(currentdir, 'auth.json')
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
        Messages are loop backed if send to clientId topic.
        You need to authorize the client to publish/subscribe to self or other opics
        1. Login to your instamsg account and edit client
        2. Add topics to Sub topics or Pub Topics
        3. For loopback add clientId to both pub and sub topics.

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
    config={
            "test":1
            }
    __publishConfig(instaMsg, config)

    
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

def __publishConfig(instaMsg, config):
    try:
        def _resultHandler(result):
            print ("Published config %s" % json.dumps(config))
    except Exception as e:
        print (str(e))
    instaMsg.publishConfig(config, _resultHandler)


def __configHandler(result):
    if(result.succeeded()):
        configJson = result.result()
        print("Received config from server: %s" % json.dumps(configJson))


def __rebootHandler():
    print("Received rebbot signal from server.")


def __getDeviceMetadata():
    return {
            'firmware_version':'',
            'programming_language':'python3.6',
            'manufacturer':'Maestro',
            'model': 'E22510', 
            'serial_number': __getSerialNumber(),
            'os':'',
            'micro_controller':{
                'make':'',
                'model':'',
                'ram':'',
                'rom':''
                },
            'cpu':{
                'make':'intel',
                'model':'i3'
                },
            'network_interfaces':[{
                'make':'',
                'model':'',
                'type':'',
                'firmware_version':'',
                'mac_address':'',
                'imei':''                  
             }],
            'memory':{
                'ram':'10',
                'rom':'100'                        
                },
            'storage':{
                'flash':1024,
                'external':''
                },
            'gps':{
                'make':'',
                'model':''                    
                }
            }

def __getSerialNumber():
    cpuserial = "0000000000000000"
    try:
        f = open('/proc/cpuinfo', 'r')
        for line in f:
            if line[0:6] == 'Serial':
                cpuserial = line[10:26]
        f.close()
    except:
        cpuserial = "ERROR00000000000"
    return cpuserial
    
if  __name__ == "__main__":
    rc = start(sys.argv)
    sys.exit(rc)
