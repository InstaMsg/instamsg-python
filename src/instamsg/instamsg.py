# -*- coding: utf-8 -*-
import time
import sys
import os
import json
import fcntl
import struct
import _thread
from threading import Thread, Event, RLock 
import socket
try:
    import ssl
    HAS_SSL = True
except:
    HAS_SSL = False  

from ..mqtt.client import *
from ..http.client import HTTPClient
from .media import MediaStream
from .errors import *

####InstaMsg ###############################################################################
# Logging Levels
INSTAMSG_LOG_LEVEL_DISABLED = 0
INSTAMSG_LOG_LEVEL_INFO = 1
INSTAMSG_LOG_LEVEL_ERROR = 2
INSTAMSG_LOG_LEVEL_DEBUG = 3
# Logging Level String
INSTAMSG_LOG_LEVEL = {0:"DISABLED", 1:"INFO", 2:"ERROR", 3:"DEBUG"}
# Error codes
INSTAMSG_ERROR_TIMEOUT = 0
INSTAMSG_ERROR_NO_HANDLERS = 1
INSTAMSG_ERROR_SOCKET = 2
INSTAMSG_ERROR_AUTHENTICATION = 3
# Message QOS
INSTAMSG_QOS0 = 0
INSTAMSG_QOS1 = 1
INSTAMSG_QOS2 = 2


class InstaMsg(Thread):
    INSTAMSG_MAX_BYTES_IN_MSG = 10240
    INSTAMSG_KEEP_ALIVE_TIMER = 60
    INSTAMSG_RECONNECT_TIMER = 90
    INSTAMSG_HOST = "device.instamsg.io"
    INSTAMSG_PORT = 1883
    INSTAMSG_PORT_SSL = 8883
    INSTAMSG_HTTP_HOST = 'platform.instamsg.io'
    INSTAMSG_HTTP_PORT = 80
    INSTAMSG_HTTPS_PORT = 443
    INSTAMSG_API_VERSION = "beta"
    INSTAMSG_RESULT_HANDLER_TIMEOUT = 10    
    INSTAMSG_MSG_REPLY_HANDLER_TIMEOUT = 10
    INSTAMSG_VERSION = "1.00.00"
    SIGNAL_PERIODIC_INTERVAL = 300
    
    def __init__(self, clientId, authKey, connectHandler, disConnectHandler, oneToOneMessageHandler, options={}):
        if(not callable(connectHandler)): raise ValueError('connectHandler should be a callable object.')
        if(not callable(disConnectHandler)): raise ValueError('disConnectHandler should be a callable object.')
        if(not callable(oneToOneMessageHandler)): raise ValueError('oneToOneMessageHandler should be a callable object.')
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
        self.__filesTopic = "instamsg/clients/" + clientId + "/files";
        self.__fileUploadUrl = "/api/%s/clients/%s/files" % (self.INSTAMSG_API_VERSION, clientId)
        self.__enableServerLoggingTopic = "instamsg/clients/" + clientId + "/enableServerLogging";
        self.__serverLogsTopic = "instamsg/clients/" + clientId + "/logs";
        self.__sessionTopic = "instamsg/client/session"
        self.__metadataTopic = "instamsg/client/metadata"
        self.__rebootTopic = "instamsg/clients/" + clientId + "/reboot"
        self.__userClientid = []
        self.__defaultReplyTimeout = self.INSTAMSG_RESULT_HANDLER_TIMEOUT
        self.__msgHandlers = {}
        self.__sendMsgReplyHandlers = {}  # {handlerId:{time:122334,handler:replyHandler, timeout:10, timeOutMsg:"Timed out"}}
        self.__manufacturer = ""
        self.__model = ""
        self.__connectivity = ""
        self.__initOptions(options)
        self.ipAddress = ''
        self.__mediaStream = None
        if(self.__enableTcp):
            clientIdAndUsername = self.__getClientIdAndUsername(clientId)
            mqttoptions = self.__mqttClientOptions(clientIdAndUsername[1], authKey, self.__keepAliveTimer, self.__connectivity)
            self.__mqttClient = MqttClient(self.INSTAMSG_HOST, self.__port, clientIdAndUsername[0], enableSsl=self.enableSsl, options=mqttoptions)
            self.__mqttClient.onConnect(self.__onConnect)
            self.__mqttClient.onDisconnect(self.__onDisConnect)
            self.__mqttClient.onDebugMessage(self.__handleDebugMessage)
            self.__mqttClient.onMessage(self.__handleMessage)
            self.__connected = 0
            self.__mqttClient.connect()
        else:
            self.__mqttClient = None
        self.__httpClient = HTTPClient(self.INSTAMSG_HTTP_HOST, self.__httpPort, enableSsl=self.enableSsl)
        
    def __initOptions(self, options):
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
            self.__keepAliveTimer = self.INSTAMSG_KEEP_ALIVE_TIMER
        
        self.enableSsl = 0 
        if('enableSsl' in options and options.get('enableSsl')): 
            if(HAS_SSL):
                self.enableSsl = 1
                self.__port = self.INSTAMSG_PORT_SSL 
                self.__httpPort = self.INSTAMSG_HTTPS_PORT
            else:
                raise ImportError("SSL not supported, Please check python version and try again.")
        else: 
            self.__port = self.INSTAMSG_PORT
            self.__httpPort = self.INSTAMSG_HTTP_PORT
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
                        self.__processHandlersTimeout()
                        self.__mqttClient.process()
                except Exception as e:
                    raise ValueError(str(e))
                    self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = process]- %s" % (str(e)))
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
                self.__mqttClient.subscribe(topic, qos, resultHandler, timeout)
            except Exception as e:
                self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = subscribe][%s]:: %s" % (e.__class__.__name__ , str(e)))
                raise InstaMsgSubError(str(e))
        else:
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = subscribe][%s]:: %s" % ("InstaMsgSubError" + str(e)))
            raise InstaMsgSubError("Cannot subscribe as TCP is not enabled. Two way messaging only possible on TCP and not HTTP")
            

    def unsubscribe(self, topics, resultHandler, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        if(self.__mqttClient):
            try:
                self.__mqttClient.unsubscribe(topics, resultHandler, timeout)
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
            
    def getIpAddress(self, ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', bytes(ifname[:15], 'utf-8'))
        )[20:24])
        
    def getSerialNumber(self):
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
    
    def streamVideo(self,mediaUrl,mediaStreamid):
        self.__mediaStream = MediaStream(self,mediaUrl, self.__clientId,mediaStreamid)
    
    def _send(self, messageId, clienId, msg, qos, dup, replyHandler, timeout):
        try:
            if(replyHandler):
                timeOutMsg = "Sending message[%s] %s to %s timed out." % (str(messageId), str(msg), str(clienId))
                self.__sendMsgReplyHandlers[messageId] = {'time':time.time(), 'timeout': timeout, 'handler':replyHandler, 'timeOutMsg':timeOutMsg}
                def _resultHandler(result):
                    if(result.failed()):
                        replyHandler(result)
                        del self.__sendMsgReplyHandlers[messageId]
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
            messageId = time.time()
        return messageId;
    
    
    def __enableServerLogging(self,msg):
        if(msg):
            msgJson = self.__parseJson(msg.body())
            if(msgJson is not None and( 'client_id' in msgJson and 'logging' in msgJson)):
                clientId = msgJson['client_id']
                logging = msgJson['logging']
                if(logging):
                    if(not self.__userClientid.__contains__(clientId)):
                        self.__userClientid.append(clientId)
                    self.__enableLogToServer = 1
                else:
                    if(self.__userClientid.__contains__(clientId)):
                        self.__userClientid.remove(clientId)
                    if(len(self.__userClientid) == 0):
                        self.__enableLogToServer = 0
    
    def __onConnect(self, mqttClient):
        self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]:: Client connected to InstaMsg IOT cloud service.")
        self.__connected = 1
        self.ipAddress = self.getIpAddress(self.__connectivity)
        self.__sendClientSessionData()
        self.__sendClientMetadata()
        self.subscribe(self.__enableServerLoggingTopic, INSTAMSG_QOS1, self.__enableServerLogging)
        time.sleep(30)
        if(self.__onConnectCallBack): self.__onConnectCallBack(self)  
        

    def __onDisConnect(self):
        self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]:: Client disconnected from InstaMsg IOT cloud service.")
        if(self.__onDisConnectCallBack): self.__onDisConnectCallBack()  
        
    def __handleDebugMessage(self, level, msg):
        if(level <= self.__logLevel):
            self.log(level, msg)
    
    def __handleMessage(self, mqttMsg):
        if(mqttMsg.topic == self.__clientId):
            self.__handleOneToOneMessage(mqttMsg)
        elif(mqttMsg.topic == self.__filesTopic):
            self.__handleFileTransferMessage(mqttMsg)
        elif(mqttMsg.topic == self.__rebootTopic):
            self.__reboot()
        else:
            msg = Message(mqttMsg.messageId, mqttMsg.topic, mqttMsg.payload, mqttMsg.fixedHeader.qos, mqttMsg.fixedHeader.dup)
            msgHandler = self.__msgHandlers.get(mqttMsg.topic)
            if(msgHandler):
                msgHandler(msg)
                
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
        
    def __mqttClientOptions(self, username, password, keepAliveTimer, connectivityMedium):
        if(len(password) > self.INSTAMSG_MAX_BYTES_IN_MSG): raise ValueError("Password length cannot be more than %d bytes." % self.INSTAMSG_MAX_BYTES_IN_MSG)
        if(keepAliveTimer > 32768 or keepAliveTimer < self.INSTAMSG_KEEP_ALIVE_TIMER): raise ValueError("keepAliveTimer should be between %d and 32768" % self.INSTAMSG_KEEP_ALIVE_TIMER)
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
        options['reconnectTimer'] = self.INSTAMSG_RECONNECT_TIMER
        options['connectivityMedium'] = connectivityMedium
        options['sendSignalInfoPeriodicInterval'] = self.SIGNAL_PERIODIC_INTERVAL
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
        for key, value in self.__sendMsgReplyHandlers.items():
            if((time.time() - value['time']) >= value['timeout']):
                resultHandler = value['handler']
                if(resultHandler):
                    timeOutMsg = value['timeOutMsg']
                    resultHandler(Result(None, 0, (INSTAMSG_ERROR_TIMEOUT, timeOutMsg)))
                    value['handler'] = None
                del self.__sendMsgReplyHandlers[key]
    
    def __sendClientSessionData(self):
        self.ipAddress = self.getIpAddress(self.__connectivity) 
        signalInfo = self.__mqttClient.getSignalInfo()
        session = {'method':self.__connectivity, 'ip_address':self.ipAddress, 'antina_status': signalInfo['antina_status'], 'signal_strength': signalInfo['signal_strength']}
#         session = {'method' : 'GPRS', 'ip_address' : '100.106.28.23', 'antina_status' : ' 1', 'signal_strength' : '31'}
        self.publish(self.__sessionTopic, str(session), 1, 0)

    def __sendClientMetadata(self):
        imei = self.getSerialNumber()
        metadata = {'imei': imei, 'serial_number': imei, 'model': self.__model,
                'firmware_version':'', 'manufacturer':self.__manufacturer, 'client_version': self.INSTAMSG_VERSION}
        self.publish(self.__metadataTopic, str(metadata), 1, 0)
        
    def __reboot(self):
        try:
            self.log(INSTAMSG_LOG_LEVEL_INFO, "Rebooting system.")
            os.system("reboot")
        except Exception as e:
            print (str(e))