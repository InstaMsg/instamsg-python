# -*- coding: utf-8 -*-
import time
import sys
import os
import json
import fcntl
import struct
import hashlib
import _thread
from threading import Thread, Event, RLock 
import socket
try:
    import ssl
    HAS_SSL = True
except:
    HAS_SSL = False  

try:
    import subprocess
    import argparse
    import re
except:
    pass

from ..mqtt.client import MqttClient
from ..http.client import HTTPClient
from ..mqtt.result import Result
from ..mqtt.constants import PROVISIONING_CLIENT_ID
from .message import Message
from .media import MediaStream
from .errors import *
from .constants import *

####InstaMsg ###############################################################################

class InstaMsg(Thread):
    
    def __init__(self, clientId, authKey, connectHandler, disConnectHandler, oneToOneMessageHandler, options={}):
        if(clientId is None): raise ValueError('clientId cannot be null.')
        if(authKey is None or authKey is ''): raise ValueError('authKey cannot be null.')
        if(not callable(connectHandler)): raise ValueError('connectHandler should be a callable object.')
        if(not callable(disConnectHandler)): raise ValueError('disConnectHandler should be a callable object.')
        if(not callable(oneToOneMessageHandler)): raise ValueError('oneToOneMessageHandler should be a callable object.')
        if(clientId): 
            if(len(clientId) != 36): raise ValueError('clientId: %s is not a valid uuid e.g. cbf7d550-7204-11e4-a2ad-543530e3bc65')% clientId
        Thread.__init__(self)
        self.name = 'InstaMsg Thread'
        self.alive = Event()
        self.alive.set()
        self.__clientId = clientId
        self.__authKey = authKey 
        self.__options = options
        self.__onConnectCallBack = connectHandler   
        self.__onDisConnectCallBack = disConnectHandler  
        self.__oneToOneMessageHandler = oneToOneMessageHandler
        self.__authHash = None
        self.__init(clientId, authKey)
        self.__logsListener = []
        self.__defaultReplyTimeout = INSTAMSG_RESULT_HANDLER_TIMEOUT
        self.__msgHandlers = {}
        self.__sendMsgReplyHandlers = {}  # {handlerId:{time:122334,handler:replyHandler, timeout:10, timeOutMsg:"Timed out"}}
        self.__sslEnabled = 0
        self.__configHandler = None
        self.__rebootHandler = None
        self.__manufacturer = ""
        self.__model = ""
        self.__connectivity = ""
        self.__ipAddress = ''
        self.__mqttClient = None
        self.__mediaStream = None
        self.__initOptions(options)
        self.__publishNetworkInfoTimer = time.time()
        if(self.__enableTcp):
            clientIdAndUsername = self.__getClientIdAndUsername(clientId)
            mqttoptions = self.__mqttClientOptions(clientIdAndUsername[1], authKey, self.__keepAliveTimer)
            self.__mqttClient = MqttClient(INSTAMSG_HOST, self.__port, clientIdAndUsername[0], enableSsl=self.enableSsl, options=mqttoptions)
            self.__mqttClient.onConnect(self.__onConnect)
            self.__mqttClient.onDisconnect(self.__onDisConnect)
            self.__mqttClient.onDebugMessage(self.__handleDebugMessage)
            self.__mqttClient.onMessage(self.__handleMessage)
            self.__connected = 0    
            self.__mqttClient.connect()
        else:
            self.__mqttClient = None
        self.__httpClient = HTTPClient(INSTAMSG_HTTP_HOST, self.__httpPort, enableSsl=self.enableSsl)
 
    def __init(self, clientId, authKey):
        if (clientId and authKey):
            self.__authHash = hashlib.sha256((clientId + authKey).encode('utf-8')).hexdigest()
            self.__filesTopic = "instamsg/clients/%s/files" % clientId
            self.__fileUploadUrl = "/api/%s/clients/%s/files" % (INSTAMSG_API_VERSION, clientId)
            self.__enableServerLoggingTopic = "instamsg/clients/%s/enableServerLogging" % clientId
            self.__serverLogsTopic = "instamsg/clients/%s/logs" % clientId
            self.__rebootTopic = "instamsg/clients/%s/reboot" % clientId
            self.__infoTopic = "instamsg/clients/%s/info" % clientId
            self.__sessionTopic = "instamsg/clients/%s/session" % clientId
            self.__metadataTopic = "instamsg/clients/%s/metadata" % clientId
            self.__configServerToClientTopic = "instamsg/clients/%s/config/serverToClient" % clientId
            self.__configClientToServerTopic = "instamsg/clients/%s/config/clientToServer" % clientId
            self.__networkInfoTopic = "instamsg/clients/%s/network" % clientId


    def __initOptions(self, options):
        if( 'configHandler' in options): 
            self.__configHandler = options['configHandler']
        if( 'rebootHandler' in options): 
            self.__rebootHandler = options['rebootHandler']
        if('enableSocket' in self.__options):
            self.__enableTcp = options.get('enableSocket')
        else: self.__enableTcp = 1
        if('enableLogToServer' in self.__options):
            self.__enableLogToServer = options.get('enableLogToServer')
        else: self.__enableLogToServer = 0
        if('logLevel' in self.__options):
            self.__logLevel = options.get('logLevel')
            if(self.__logLevel < INSTAMSG_LOG_LEVEL_DISABLED or self.__logLevel > INSTAMSG_LOG_LEVEL_DEBUG):
                raise ValueError("logLevel option should be in between %d and %d" % (INSTAMSG_LOG_LEVEL_DISABLED, INSTAMSG_LOG_LEVEL_DEBUG))
        else: self.__logLevel = INSTAMSG_LOG_LEVEL_DISABLED
        if('keepAliveTimer' in options):
            self.__keepAliveTimer = options.get('keepAliveTimer')
        else:
            self.__keepAliveTimer = INSTAMSG_KEEP_ALIVE_TIMER
        
        self.enableSsl = 0 
        if('enableSsl' in options and options.get('enableSsl')): 
            if(HAS_SSL):
                self.enableSsl = 1
                self.__port = INSTAMSG_PORT_SSL 
                self.__httpPort = INSTAMSG_HTTPS_PORT
            else:
                raise ImportError("SSL not supported, Please check python version and try again.")
        else: 
            self.__port = INSTAMSG_PORT
            self.__httpPort = INSTAMSG_HTTP_PORT
        if("connectivity" in options):
            self.__connectivity = options.get("connectivity");
        else:
            self.__connectivity = "eth0"
        if("manufacturer" in options):
            self.__manufacturer = options.get("manufacturer");
        if("model" in options):
            self.__model = options.get("model");
    
    def run(self):
        try:
            while self.alive.isSet():
                try:
                    if(self.__mqttClient is not None):
                        self.__mqttClient.process()
                        self.__processHandlersTimeout()
                        self.__publishNetworkInfo()
                except Exception as e:
                    self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = run]- %s" % (str(e)))
        finally:
            self.close()
                
    def close(self):
        try:
            if(self.__mqttClient is not None):
                self.__mqttClient.disconnect()
                self.__mqttClient = None
            self.__sendMsgReplyHandlers = None
            self.__msgHandlers = None
            self.__subscribers = None
            self.alive.clear()
            self.join()
            return 1
        except:
            return -1
    
    def publish(self, topic, msg, qos=INSTAMSG_QOS0, dup=0, resultHandler=None, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT, logging=1):
        if(topic):
            try:
                self.__mqttClient.publish(topic, msg, qos, dup, resultHandler, timeout, logging=logging)
            except Exception as e:
                raise InstaMsgPubError(str(e))
        else: raise ValueError("Topic cannot be null or empty string.")
    
    def subscribe(self, topic, qos, msgHandler, resultHandler=None, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        if(self.__mqttClient):
            try:
                if(not callable(msgHandler)): raise ValueError('msgHandler should be a callable object.')
                self.__msgHandlers[topic] = msgHandler
                if(topic == self.__clientId):
                    raise ValueError("Canot subscribe to clientId. Instead set oneToOneMessageHandler.")
                def _resultHandler(result):
                    if(result.failed()):
                        if(topic in self.__msgHandlers):
                            del self.__msgHandlers[topic]
                    if(callable(resultHandler)): resultHandler(result)
                self.__mqttClient.subscribe(topic, qos, _resultHandler, timeout)
            except Exception as e:
                self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = subscribe][%s]:: %s" % (e.__class__.__name__ , str(e)))
                raise InstaMsgSubError(str(e))
        else:
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = subscribe][%s]:: %s" % ("InstaMsgSubError" + str(e)))
            raise InstaMsgSubError("Cannot subscribe as TCP is not enabled. Two way messaging only possible on TCP and not HTTP")
            

    def unsubscribe(self, topics, resultHandler, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        if(self.__mqttClient):
            try:
                def _resultHandler(result):
                    if(result.succeeded()):
                        for topic in topics:
                            if(topic in self.__msgHandlers):
                                del self.__msgHandlers[topic]
                    resultHandler(result)
                self.__mqttClient.unsubscribe(topics, _resultHandler, timeout)
            except Exception as e:
                self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = unsubscribe][%s]:: %s" % (e.__class__.__name__ , str(e)))
                raise InstaMsgUnSubError(str(e))
        else:
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = unsubscribe][%s]:: %s" % ("InstaMsgUnSubError" , str(e)))
            raise InstaMsgUnSubError("Cannot unsubscribe as TCP is not enabled. Two way messaging only possible on TCP and not HTTP")
    
    def send(self, clienId, msg, qos=INSTAMSG_QOS0, dup=0, replyHandler=None, timeout=INSTAMSG_MSG_REPLY_HANDLER_TIMEOUT):
        try:
            messageId = self._generateMessageId()
            msg = Message(messageId, clienId, msg, qos, dup, replyTopic=self.__clientId, instaMsg=self)._sendMsgJsonString()
            self._send(messageId, clienId, msg, qos, dup, replyHandler, timeout)
        except Exception as e:
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = send][%s]:: %s" % (e.__class__.__name__ , str(e)))
            raise InstaMsgSendError(str(e))
        
    def log(self, level, message):
        if(self.__enableLogToServer and self.__mqttClient.connected()):
            self.publish(self.__serverLogsTopic, message, 1, 0, logging=0)
        else:
            timeString = time.strftime("%d/%m/%Y, %H:%M:%S:%z")
            print ("[%s] - [%s] - [%s]%s" % (_thread.get_ident(), timeString, INSTAMSG_LOG_LEVEL[level], message))
   
    @classmethod        
    def provision(cls, provId, provPin, provisionHandler, timeout = 60):
        if(not callable(provisionHandler)): raise ValueError('provisionHandler should be a callable object.')
        try:
            def _log(level, message):
                timeString = time.strftime("%d/%m/%Y, %H:%M:%S:%z")
                print ("[%s] - [%s] - [%s]%s" % (_thread.get_ident(), timeString, INSTAMSG_LOG_LEVEL[level], message))               
            _log(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]::Sending provisioning message...")      
            mqttClient = MqttClient(INSTAMSG_HOST, INSTAMSG_PORT_SSL, PROVISIONING_CLIENT_ID, enableSsl=1)
            mqttClient.onDebugMessage(_log)
            provResponse = mqttClient.provision(provId, provPin, timeout)
            if(provResponse):
                provisionHandler (provResponse)
                _log(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]::Provisioning completed.")
            else:
                _log(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]::Provisioning failed.")
        except Exception as e:
            _log(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = provisoin]- %s" % (str(e)))    
            raise InstaMsgProvisionError(str(e))


    # def publishConfig(self, configs, resultHandler=None):
    #     if(not self.__provisioned):InstaMsgSendError("Cannot publish config as device not provisioned.")
    #     try:
    #         configs["instamsg_version"]="1.0"
    #         message = str(configs).replace("\'", '"').replace("'", '"')
    #         def _resultHandler(result):
    #             if(result.failed()):
    #                 if(callable(resultHandler)):resultHandler(Result(configs,0,result.cause()))  
    #                 self.log(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsg]::Error publishing Config to server: %s" %str(result.cause()))  
    #             else:
    #                 if(callable(resultHandler)):resultHandler(Result(configs,1))
    #                 self.log(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]::Config published to server: %s" %str(configs))  
    #         self.publish(self.__configClientToServerTopic, message, qos=INSTAMSG_QOS1, dup=0, resultHandler=_resultHandler)
    #     except Exception, e:
    #         self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientConfigError, method = send][%s]:: %s" % (e.__class__.__name__ , str(e)))

      
  
    def streamVideo(self,mediaUrl,mediaStreamid):
        self.__mediaStream = MediaStream(self,mediaUrl, self.__clientId,mediaStreamid)

    def __getIpAddress(self, ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', bytes(ifname[:15], 'utf-8'))
        )[20:24])
        
    def __getSerialNumber(self):
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
    
    def _send(self, messageId, clienId, msg, qos, dup, replyHandler, timeout):
        try:
            if(replyHandler):
                timeOutMsg = "Sending message[%s] %s to %s timed out." % (str(messageId), str(msg), str(clienId))
                self.__sendMsgReplyHandlers[messageId] = {'time':time.time(), 'timeout': timeout, 'handler':replyHandler, 'timeOutMsg':timeOutMsg}
                def _resultHandler(result):
                    if(result.failed()):
                        if(messageId in self.__sendMsgReplyHandlers):
                            del self.__sendMsgReplyHandlers[messageId]
                    replyHandler(result)
            else:
                _resultHandler = None
            self.publish(clienId, msg, qos, dup, _resultHandler)
        except Exception as e:
            if(messageId in self.__sendMsgReplyHandlers):
                del self.__sendMsgReplyHandlers[messageId]
            raise Exception(str(e))
            
    def _generateMessageId(self):
        messageId = self.__clientId + "-" + str(int(time.time() * 1000))
        while(messageId in self.__sendMsgReplyHandlers):
            messageId = self.__clientId + "-" + str(int(time.time() * 1000))
        return messageId;
 

    def __publishNetworkInfo(self):
        if(self.__publishNetworkInfoTimer - time.time() <= 0):
            networkInfo = self.__getSignalInfo()
            self.publish(self.__networkInfoTopic, str(networkInfo), INSTAMSG_QOS0, 0)
            self.__publishNetworkInfoTimer = self.__publishNetworkInfoTimer + NETWORK_INFO_PUBLISH_INTERVAL

    def __getSignalInfo(self):
        result = {'antenna_status':'', 'signal_strength':''}
        try :
            parser = argparse.ArgumentParser(description='Display WLAN signal strength.')
            parser.add_argument(dest='interface', nargs='?', default=self.__connectivity,
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
                            'antenna_status':linkQuality, 
                            'signal_strength':signalLevel, 
                            "instamsg_version" : INSTAMSG_VERSION
                            }
                elif 'Not-Associated' in line:
                    self.__log(INSTAMSG_LOG_LEVEL_DEBUG, "[InstaMsg, method = getSignalInfo]:: No signal.") 
        except Exception as msg:
            self.__log(INSTAMSG_LOG_LEVEL_DEBUG, "[InstaMsg, method = getSignalInfo][%s]:: %s" % (msg.__class__.__name__ , str(msg)))
        return result;
    
    def __enableServerLogging(self, msg):
        if (msg):
            msgJson = self.__parseJson(msg.payload);
            if (msgJson is not None and ('client_id' in msgJson and ('logging' in msgJson))):
                clientId = str(msgJson['client_id'])
                logging = msgJson['logging']
                if (logging):
                    if(clientId not in self.__logsListener):
                        self.__logsListener.append(clientId)
                        self.__enableLogToServer = 1;
                else:
                    if(clientId in self.__logsListener):
                        self.__logsListener.remove(clientId);
                    if (len(self.__logsListener) == 0):
                        self.__enableLogToServer = 0;
    
    def __onConnect(self, mqttClient):
        self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]:: Client connected to InstaMsg IOT cloud service.")
        self.__connected = 1
        self.__ipAddress = self.__getIpAddress(self.__connectivity)
        self.__sendClientSessionData()
        self.__sendClientMetadata()
        self.subscribe(self.__enableServerLoggingTopic, INSTAMSG_QOS0, self.__enableServerLogging)
        time.sleep(30)
        if(self.__onConnectCallBack): self.__onConnectCallBack(self)  
        

    def __onDisConnect(self):
        self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]:: Client disconnected from InstaMsg IOT cloud service.")
        if(self.__onDisConnectCallBack): self.__onDisConnectCallBack()  
        
    def __handleDebugMessage(self, level, msg):
        if(level <= self.__logLevel):
            self.log(level, msg)
    
    def __handleMessage(self, mqttMsg):
        try:
            if(mqttMsg.topic == self.__clientId):
                self.__handlePointToPointMessage(mqttMsg)
            elif(mqttMsg.topic == self.__filesTopic):
                self.__handleFileTransferMessage(mqttMsg)
            elif(mqttMsg.topic == self.__rebootTopic):
                self.__handleSystemRebootMessage()
            elif(mqttMsg.topic == self.__configServerToClientTopic):
                self.__handleConfigMessage(mqttMsg)
            elif(mqttMsg.topic == self.__enableServerLoggingTopic):
                self.__enableServerLogging(mqttMsg)
            else:
                msg = Message(mqttMsg.messageId, mqttMsg.topic, mqttMsg.payload, mqttMsg.fixedHeader.qos, mqttMsg.fixedHeader.dup)
                msgHandler = self.__msgHandlers.get(mqttMsg.topic)
                if(msgHandler):
                    msgHandler(msg)
        except Exception as e:
            self.log(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsg]::Error in handling received message. Error message is : %s " % str(e))
                
    def __handleFileTransferMessage(self, mqttMsg):
        msgJson = self.__parseJson(mqttMsg.payload)
        qos, dup = mqttMsg.fixedHeader.qos, mqttMsg.fixedHeader.dup
        messageId, replyTopic, method, url, filename = None, None, None, None, None
        if('reply_to' in msgJson):
            replyTopic = msgJson['reply_to']
        else:
            raise ValueError("File transfer message json should have reply_to address.")   
        if('message_id' in msgJson):
            messageId = msgJson['message_id']
        else: 
            raise ValueError("File transfer message json should have a message_id.") 
        if('method' in msgJson):
            method = msgJson['method']
        else: 
            raise ValueError("File transfer message json should have a method.") 
        if('url' in msgJson):
            url = msgJson['url']
        if('filename' in msgJson):
            filename = msgJson['filename']
        if(replyTopic):
            if(method == "GET" and not filename):
                filelist = self.__getFileList()
                msg = '{"response_id": "%s", "status": 1, "files": %s}' % (messageId, filelist)
                self.publish(replyTopic, msg, qos, dup)
            elif (method == "GET" and filename):
                httpResponse = self.__httpClient.uploadFile(self.__fileUploadUrl, filename, headers={"Authorization":self.__authKey, "ClientId":self.__clientId})
                if(httpResponse and httpResponse.status == 200):
                    msg = '{"response_id": "%s", "status": 1, "url":"%s"}' % (messageId, httpResponse.body)
                else:
                    msg = '{"response_id": "%s", "status": 0}' % (messageId)
                self.publish(replyTopic, msg, qos, dup)
            elif ((method == "POST" or method == "PUT") and filename and url):
                httpResponse = self.__httpClient.downloadFile(url, filename)
                if(httpResponse and httpResponse.status == 200):
                    msg = '{"response_id": "%s", "status": 1}' % (messageId)
                else:
                    msg = '{"response_id": "%s", "status": 0}' % (messageId)
                self.publish(replyTopic, msg, qos, dup)
            elif ((method == "DELETE") and filename):
                try:
                    msg = '{"response_id": "%s", "status": 1}' % (messageId)
                    self.__deleteFile(filename)
                    self.publish(replyTopic, msg, qos, dup)
                except Exception as e:
                    msg = '{"response_id": "%s", "status": 0, "error_msg":"%s"}' % (messageId, str(e))
                    self.publish(replyTopic, msg, qos, dup)
                    
            
    def __getFileList(self):
        path = os.getcwd()
        fileList = ""
        for root, dirs, files in os.walk(path):
            for name in files:
                filename = os.path.join(root, name)
                size = os.stat(filename).st_size
                if(fileList):
                    fileList = fileList + ","
                fileList = fileList + '"%s":%d' % (name, size)
        return '{%s}' % fileList      
    
    def __deleteFile(self, filename):
        path = os.getcwd()
        filename = os.path.join(path, filename)
        os.remove(filename)
        
        
    def __handleOneToOneMessage(self, mqttMsg):
        msgJson = self.__parseJson(mqttMsg.payload)
        messageId, responseId, replyTopic, status = None, None, None, 1
        if('reply_to' in msgJson):
            replyTopic = msgJson['reply_to']
        else:
            raise ValueError("Send message json should have reply_to address.")   
        if('message_id' in msgJson):
            messageId = msgJson['message_id']
        else: 
            raise ValueError("Send message json should have a message_id.") 
        if('response_id' in msgJson):
            responseId = msgJson['response_id']
        if('body' in msgJson):
            body = msgJson['body']
        if('status' in msgJson):
            status = int(msgJson['status'])
        qos, dup = mqttMsg.fixedHeader.qos, mqttMsg.fixedHeader.dup
        if(responseId):
            # This is a response to existing message
            if(status == 0):
                errorCode, errorMsg = None, None
                if(isinstance(body, dict)):
                    if("error_code" in body):
                        errorCode = body.get("error_code")
                    if("error_msg" in body):
                        errorMsg = body.get("error_msg")
                result = Result(None, 0, (errorCode, errorMsg))
            else:
                msg = Message(messageId, self.__clientId, body, qos, dup, replyTopic=replyTopic, instaMsg=self)
                result = Result(msg, 1)
            
            if(responseId in self.__sendMsgReplyHandlers):
                msgHandler = self.__sendMsgReplyHandlers.get(responseId).get('handler')
            else:
                msgHandler = None
                self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]:: No handler for message [messageId=%s responseId=%s]" % (str(messageId), str(responseId)))
            if(msgHandler):
                msgHandler(result)
                del self.__sendMsgReplyHandlers[responseId]
        else:
            if(self.__oneToOneMessageHandler):
                msg = Message(messageId, self.__clientId, body, qos, dup, replyTopic=replyTopic, instaMsg=self)
                self.__oneToOneMessageHandler(msg)
        
    def __mqttClientOptions(self, username, password, keepAliveTimer):
        if(len(password) > INSTAMSG_MAX_BYTES_IN_MSG): raise ValueError("Password length cannot be more than %d bytes." % INSTAMSG_MAX_BYTES_IN_MSG)
        if(keepAliveTimer > 32768 or keepAliveTimer < INSTAMSG_KEEP_ALIVE_TIMER): raise ValueError("keepAliveTimer should be between %d and 32768" % INSTAMSG_KEEP_ALIVE_TIMER)
        options = {}
        options['hasUserName'] = 1
        options['hasPassword'] = 1
        options['username'] = username
        options['password'] = password
        options['isCleanSession'] = 1
        options['keepAliveTimer'] = keepAliveTimer
        options['isWillFlag'] = 0
        options['willQos'] = 0
        options['isWillRetain'] = 0
        options['willTopic'] = ""
        options['willMessage'] = ""
        options['logLevel'] = self.__logLevel
        options['reconnectTimer'] = INSTAMSG_RECONNECT_TIMER
        return options
    
    def __getClientIdAndUsername(self, clientId):
        errMsg = 'clientId is not a valid uuid e.g. cbf7d550-7204-11e4-a2ad-543530e3bc65'
        if(clientId is None): raise ValueError('clientId cannot be null.')
        if(len(clientId) != 36): raise ValueError(errMsg)
        c = clientId.split('-')
        if(len(c) != 5): raise ValueError(errMsg)
        cId = '-'.join(c[0:4])
        userName = c[4 ]
        if(len(userName) != 12): raise ValueError(errMsg)
        return (cId, userName)
    
    def __parseJson(self, jsonString):
#         return eval(jsonString)  # Hack as not implemented Json Library
        return json.loads(jsonString)
    
    def __processHandlersTimeout(self): 
        for key in list(self.__resultHandlers):
            value = self.__resultHandlers[key]
            if((time.time() - value['time']) >= value['timeout']):
                resultHandler = value['handler']
                if(resultHandler):
                    timeOutMsg = value['timeOutMsg']
                    resultHandler(Result(None, 0, (INSTAMSG_ERROR_TIMEOUT, timeOutMsg)))
                    value['handler'] = None
                del self.__sendMsgReplyHandlers[key]
    
    def __sendClientSessionData(self):
        self.__ipAddress = self.__getIpAddress(self.__connectivity) 
        signalInfo = self.__getSignalInfo()
        session = {'network_interface':self.__connectivity, 'ip_address':self.__ipAddress, 'antenna_status': signalInfo['antenna_status'], 'signal_strength': signalInfo['signal_strength']}
#         session = {'method' : 'GPRS', 'ip_address' : '100.106.28.23', 'antenna_status' : ' 1', 'signal_strength' : '31'}
        self.publish(self.__sessionTopic, str(session), INSTAMSG_QOS0, 0)

    def __sendClientMetadata(self):
        imei = self.__getSerialNumber()
        metadata = {
                'imei': imei, 'serial_number': imei, 'model': self.__model,
                'manufacturer':self.__manufacturer, 
                'firmware_version':'', 
                'client_version': INSTAMSG_VERSION,
                "instamsg_version" : INSTAMSG_VERSION
                }
        self.publish(self.__metadataTopic, str(metadata), INSTAMSG_QOS0, 0)
    
    def __handleConfigMessage(self, mqttMsg):
        msgJson = self.__parseJson(mqttMsg.payload)
        if(callable(self.__configHandler)):
            self.__configHandler(Result((msgJson),1))

    def __handleSystemRebootMessage(self):
        self.log(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]::Rebooting device.")
        if(callable(self.__rebootHandler)):
            self.__rebootHandler() 