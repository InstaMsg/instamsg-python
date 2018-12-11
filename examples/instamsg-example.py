import os,sys
import json
import time
import inspect
import random
import subprocess
import argparse
import re


#add parent directory to path so that modules can be imported when example 
#script directly run from current folder 
#$ python path/to/examples/instamsg-example.py
#or via $make run

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

from src.instamsg import instamsg

clientId = ''
authKey = ''
INSTAMSG_CONNECTED = False

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
        instaMsg = _startInstaMsg(provId, provkey)
        if(instaMsg):
            networkInfoPublishInterval = 10
            publishNetworkInfoTimer = time.time()
            while 1:
                # Periodically publish network info to InstaMsg cloud
                if(INSTAMSG_CONNECTED and publishNetworkInfoTimer - time.time() <= 0):
                    try:
                        networkInfo = _getNetworkInfo("wlan0")
                        if(networkInfo): instaMsg.publishNetworkInfo(networkInfo)
                    except Exception as e:
                        print("Error while publishing periodic network info: %s" % str(e))
                    finally:
                        publishNetworkInfoTimer = publishNetworkInfoTimer + networkInfoPublishInterval
                time.sleep(10) #
    except:
       print("Unknown Error in start: %s %s" % (str(sys.exc_info()[0]), str(sys.exc_info()[1]))) 
    finally:
        instaMsg = None


def _startInstaMsg(provId='', provkey=''):
    options = {
                'logLevel':instamsg.INSTAMSG_LOG_LEVEL_INFO, 
                'enableTcp':1, # 1 TCP 0 WebSocket
                'enableSsl':0, 
                'configHandler': _configHandler,
                'rebootHandler': _rebootHandler,
                'metadata': _getDeviceMetadata()
                }
    # Try to get auth info from auth.json if file exists
    try:
        global clientId, authKey
        instaMsg = None
        auth = _getAuthJson()
        clientId = auth['client_id']
        authKey = auth['auth_token']
        enable_client_side_ssl_certificate = auth['auth_token']
        client_ssl_certificate = auth['certificate']
        client_ssl_certificate_key = auth['key']
        instaMsg = instamsg.InstaMsg(clientId, authKey, _onConnect, _onDisConnect, _oneToOneMessageHandler, options)
        instaMsg.start()  
        return instaMsg       
    except (IOError, FileNotFoundError):
        print("File auth.json not found or path is incorrect. Trying provisioning...")
        instaMsg =  instamsg.InstaMsg.provision(provId, provkey, _provisionHandler, enableSsl=0)
    finally:
        return instaMsg

def _getAuthJson():
    auth = None
    print("Trying to read auth info from auth.json ...")
    currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) 
    filename =  os.path.join(currentdir, 'auth.json')
    with open(filename,"r") as f:
        auth = json.load(f)
    f.close() 
    return auth   


def _provisionHandler(provMsg):
    """
    1. On succesfull provisioning a file auth.json would be created in 
       current working directory.
    2. These credentials would be used to connect to InstaMsg cloud.
    3. If you loose these credentials you will have to login into your InstamSg
       account and reprovision the client with new provisioning pin.
    """
    print(" Received provisioning response %s . Saving to file auth.json" % provMsg) 
    try:
        currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) 
        filename =  os.path.join(currentdir, 'auth.json')
        with open(filename,"w+") as f:
            json.dump(provMsg ,f)
        f.close()
        return _startInstaMsg()
    except IOError:
        print("File not found or path is incorrect")


def _onConnect(instaMsg):
    global INSTAMSG_CONNECTED
    INSTAMSG_CONNECTED = True
    topic = "subtopic1"
    qos = 0
    _publishMessage(instaMsg, "instamsg/webhook", "Test message 1",1, 0)
    time.sleep(1)
    _publishMessage(instaMsg, "instamsg/webhook", "Test message 2",1, 0)
    time.sleep(1)
    _publishMessage(instaMsg, "instamsg/webhook", "Test message 3",1, 0)
    time.sleep(1)
    _subscribe(instaMsg, topic, qos)
    try:
        """
        Messages are loop backed if send to clientId topic.
        You need to authorize the client to publish/subscribe to self or other opics
        1. Login to your instamsg account and edit client
        2. Add topics to Sub topics or Pub Topics
        3. For loopback add clientId to both pub and sub topics.

        """

        print("Sending loopbak messages to self...")
        auth = _getAuthJson()
        clientId = auth['client_id']
        authKey = auth['auth_token']
        _publishMessage(instaMsg, clientId, "Test message 4",2, 0)
        time.sleep(1)
        _sendMessage(instaMsg, clientId)
        time.sleep(1)
        _publishMessage(instaMsg, clientId, "Test message 6",0, 0)
    except IOError:
        print("File auth.json not found or path is incorrect. Unable to send loopbak messages to self...")
    time.sleep(10)
    _unsubscribe(instaMsg, topic)
    config={
            "test":1
            }
    _publishConfig(instaMsg, config)

    
def _onDisConnect():
    global INSTAMSG_CONNECTED
    INSTAMSG_CONNECTED = False
    print ("Client disconnected.")
    
def _subscribe(instaMsg, topic, qos):  
    try:
        def _resultHandler(result):
            print ("Subscribed to topic %s with qos %d" % (topic, qos))
        instaMsg.subscribe(topic, qos, _messageHandler, _resultHandler)
    except Exception as e:
        print (str(e))
    
def _publishMessage(instaMsg, topic, msg, qos, dup):
    try:
        def _resultHandler(result):
            print (result)
            print ("Published message %s to topic %s with qos %d" % (msg, topic, qos))
        instaMsg.publish(topic, msg, qos, dup, resultHandler=_resultHandler)
    except Exception as e:
        print (str(e))
    
def _unsubscribe(instaMsg, topic):
    try:
        def _resultHandler(result):
            print ("UnSubscribed from topic %s" % topic)
        instaMsg.unsubscribe(topic, _resultHandler)
    except Exception as e:
        print (str(e))
        
def _messageHandler(mqttMessage):
        if(mqttMessage):
            print ("Received message %s" % str(mqttMessage.toString()))
        
def _oneToOneMessageHandler(msg):
    if(msg):
        print ("One to One Message received %s" % msg.toString())
        msg.reply("This is a reply to a one to one message.")
        
def _sendMessage(instaMsg, clientId):
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

def _publishConfig(instaMsg, config):
    try:
        def _resultHandler(result):
            print ("Published config %s" % json.dumps(config))
    except Exception as e:
        print (str(e))
    instaMsg.publishConfig(config, _resultHandler)


def _configHandler(result):
    if(result.succeeded()):
        configJson = result.result()
        print("Received config from server: %s" % json.dumps(configJson))


def _rebootHandler():
    print("Received rebbot signal from server.")


def _getDeviceMetadata():
    return {
            'firmware_version':'',
            'programming_language':'python3.6',
            'manufacturer':'Maestro',
            'model': 'E22510', 
            'serial_number': _getSerialNumber(),
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

def _getIpAddress(interfaceName):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', bytes(interfaceName[:15], 'utf-8'))
    )[20:24])


def _getNetworkInfo(interfaceName):
    result = None
    try :
        parser = argparse.ArgumentParser(description='Display WLAN signal strength.')
        parser.add_argument(dest='interface', nargs='?', 
                            default=interfaceName,
                            help='wlan interface (default: wlan0)')
        args = parser.parse_args()
        cmd = subprocess.Popen('iwconfig %s' % args.interface, shell=True,
                           stdout=subprocess.PIPE)
        for line in cmd.stdout:
            line = line.decode("utf-8")
            if 'Link Quality' in line:
                linkQuality = re.search('Link Quality=(.+? )', line).group(1)
                signalLevel = re.search('Signal level=(.+?) dBm', line).group(1)
                result = {
                        'network_interface': interfaceName,
                        'antenna_status':linkQuality, 
                        'signal_strength':signalLevel,
                        'mac_id':'',
                        'imei':'',
                        'msisdn':'',
                        'iccid':''
                        }
            elif 'Not-Associated' in line:
                print("No signal information.")
            # No information mock for testing
            if(not result):
                result = {
                        'network_interface': interfaceName,
                        'antenna_status':'1', 
                        'signal_strength':(-1 * random.randint(0,100)),
                        'mac_id':'00:16:B6:C5:0C:FF',
                        'imei':'',
                        'msisdn':'',
                        'iccid':''
                        } 
    except Exception as msg:
        print("Error getting network interface info- %s" % (traceback.print_exc()))
    finally:
        return result;

def _getSerialNumber():
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
