# -*- coding: utf-8 -*-
import time
import socket
import OpenSSL.crypto
import sys
import os
import json
import fcntl
import struct
from threading import Thread, Event, RLock 
from duplicity.gpginterface import Pipe

try:
    import ssl
    HAS_SSL = True
except:
    HAS_SSL=  False

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
    INSTAMSG_HOST = "localhost"
    INSTAMSG_PORT = 1883
    INSTAMSG_PORT_SSL = 8883
    INSTAMSG_HTTP_HOST = 'platform.instamsg.io'
    INSTAMSG_HTTP_PORT = 80
    INSTAMSG_HTTPS_PORT = 443
    INSTAMSG_API_VERSION = "beta"
    INSTAMSG_RESULT_HANDLER_TIMEOUT = 10    
    INSTAMSG_MSG_REPLY_HANDLER_TIMEOUT = 10
    
    def __init__(self, clientId, authKey, connectHandler, disConnectHandler, oneToOneMessageHandler, options={}):
        if(not callable(connectHandler)): raise ValueError('connectHandler should be a callable object.')
        if(not callable(disConnectHandler)): raise ValueError('disConnectHandler should be a callable object.')
        if(not callable(oneToOneMessageHandler)): raise ValueError('oneToOneMessageHandler should be a callable object.')
        Thread.__init__(self)
        self.name='InstaMsg Thread'
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
        self.__serverLogsTopic =  "instamsg/clients/" + clientId + "/logs";
        self.__sessionTopic = "instamsg/client/session"
        self.__metadataTopic = "instamsg/client/metadata"
        self.__rebootTopic = "instamsg/clients/" + clientId + "/reboot"
        self.__userClientid = []
        self.__defaultReplyTimeout = self.INSTAMSG_RESULT_HANDLER_TIMEOUT
        self.__msgHandlers = {}
        self.__sendMsgReplyHandlers = {}  # {handlerId:{time:122334,handler:replyHandler, timeout:10, timeOutMsg:"Timed out"}}
        self.__initOptions(options)
        if(self.__enableTcp):
            clientIdAndUsername = self.__getClientIdAndUsername(clientId)
            mqttoptions = self.__mqttClientOptions(clientIdAndUsername[1], authKey, self.__keepAliveTimer)
            self.__mqttClient = MqttClient(self.INSTAMSG_HOST, self.__port, clientIdAndUsername[0], mqttoptions)
            self.__mqttClient.onConnect(self.__onConnect)
            self.__mqttClient.onDisconnect(self.__onDisConnect)
            self.__mqttClient.onDebugMessage(self.__handleDebugMessage)
            self.__mqttClient.onMessage(self.__handleMessage)
            self.__connected =0
            self.__mqttClient.connect()
        else:
            self.__mqttClient = None
        self.__httpClient = HTTPClient(self.INSTAMSG_HTTP_HOST, self.__httpPort,enableSsl = self.enableSsl)
        
    def __initOptions(self, options):
        if(self.__options.has_key('enableSocket')):
            self.__enableTcp = options.get('enableSocket')
        else: self.__enableTcp = 1
        if(self.__options.has_key('enableLogToServer')):
            self.__enableLogToServer = options.get('enableLogToServer')
        else: self.__enableLogToServer = 0
        if(self.__options.has_key('logLevel')):
            self.__logLevel = options.get('logLevel')
            if(self.__logLevel < INSTAMSG_LOG_LEVEL_DISABLED or self.__logLevel > INSTAMSG_LOG_LEVEL_DEBUG):
                raise ValueError("logLevel option should be in between %d and %d" % (INSTAMSG_LOG_LEVEL_DISABLED, INSTAMSG_LOG_LEVEL_DEBUG))
        else: self.__logLevel = INSTAMSG_LOG_LEVEL_DISABLED
        if(options.has_key('keepAliveTimer')):
            self.__keepAliveTimer = options.get('keepAliveTimer')
        else:
            self.__keepAliveTimer = self.INSTAMSG_KEEP_ALIVE_TIMER
        
        self.enableSsl = 0 
        if(options.has_key('enableSsl') and options.get('enableSsl')): 
            if(HAS_SSL):
                self.enableSsl = 1
                self.__port = self.INSTAMSG_PORT_SSL 
                self.__httpPort = self.INSTAMSG_HTTPS_PORT
            else:
                raise ImportError("SSL not supported, Please check python version and try again.")
        else: 
            self.__port = self.INSTAMSG_PORT
            self.__httpPort = self.INSTAMSG_HTTP_PORT
    
    def run(self):
        try:
            while self.alive.isSet():
                try:
                    if(self.__mqttClient):
                        self.__processHandlersTimeout()
                        self.__mqttClient.process()
                except Exception, e:
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
    
    def publish(self, topic, msg, qos=INSTAMSG_QOS0, dup=0, resultHandler=None, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT,logging =1):
        if(topic):
            try:
                self.__mqttClient.publish(topic, msg, qos, dup, resultHandler, timeout,logging=logging)
            except Exception, e:
                raise InstaMsgPubError(str(e))
        else: raise ValueError("Topic cannot be null or empty string.")
    
    def subscribe(self, topic, qos, msgHandler, resultHandler, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        if(self.__mqttClient):
            try:
                if(not callable(msgHandler)): raise ValueError('msgHandler should be a callable object.')
                self.__msgHandlers[topic] = msgHandler
                if(topic == self.__clientId):
                    raise ValueError("Canot subscribe to clientId. Instead set oneToOneMessageHandler.")
                self.__mqttClient.subscribe(topic, qos, resultHandler, timeout)
            except Exception, e:
                self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = subscribe][%s]:: %s" % (e.__class__.__name__ , str(e)))
                raise InstaMsgSubError(str(e))
        else:
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = subscribe][%s]:: %s" % ("InstaMsgSubError" + str(e)))
            raise InstaMsgSubError("Cannot subscribe as TCP is not enabled. Two way messaging only possible on TCP and not HTTP")
            

    def unsubscribe(self, topics, resultHandler, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        if(self.__mqttClient):
            try:
                self.__mqttClient.unsubscribe(topics, resultHandler, timeout)
            except Exception, e:
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
        except Exception, e:
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = send][%s]:: %s" % (e.__class__.__name__ , str(e)))
            raise InstaMsgSendError(str(e))
     
    def log(self, level, message):
        time.sleep(10)
        if(self.__enableLogToServer and self.__mqttClient.connected()):
            self.publish(self.__serverLogsTopic, message, 1, 0, logging=0)
        else:
            print "[%s]%s" % (INSTAMSG_LOG_LEVEL[level], message)
    
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
        except Exception, e:
            if(self.__sendMsgReplyHandlers.has_key(messageId)):
                del self.__sendMsgReplyHandlers[messageId]
            raise Exception(str(e))
            
    def _generateMessageId(self):
        messageId = self.__clientId + "-" + str(int(time.time() * 1000))
        while(self.__sendMsgReplyHandlers.has_key(messageId)):
            messageId = time.time()
        return messageId;
    
    
    def __enableServerLogging(self,msg):
        if(msg):
            msgJson = self.__parseJson(msg.body())
            if(msgJson is not None and( msgJson.has_key('client_id') and msgJson.has_key('logging'))):
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
        self.__connected =1
        self.__mqttClient.subscribe(self.__enableServerLoggingTopic, 1, self.__enableServerLogging)
        if(self.__onConnectCallBack): self.__onConnectCallBack(self)  
        self.__sendClientSessionData()
        self.__sendClientMetadata()

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
        if(msgJson.has_key('reply_to')):
            replyTopic = msgJson['reply_to']
        else:
            raise ValueError("File transfer message json should have reply_to address.")   
        if(msgJson.has_key('message_id')):
            messageId = msgJson['message_id']
        else: 
            raise ValueError("File transfer message json should have a message_id.") 
        if(msgJson.has_key('method')):
            method = msgJson['method']
        else: 
            raise ValueError("File transfer message json should have a method.") 
        if(msgJson.has_key('url')):
            url = msgJson['url']
        if(msgJson.has_key('filename')):
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
                except Exception, e:
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
        if(msgJson.has_key('reply_to')):
            replyTopic = msgJson['reply_to']
        else:
            raise ValueError("Send message json should have reply_to address.")   
        if(msgJson.has_key('message_id')):
            messageId = msgJson['message_id']
        else: 
            raise ValueError("Send message json should have a message_id.") 
        if(msgJson.has_key('response_id')):
            responseId = msgJson['response_id']
        if(msgJson.has_key('body')):
            body = msgJson['body']
        if(msgJson.has_key('status')):
            status = int(msgJson['status'])
        qos, dup = mqttMsg.fixedHeader.qos, mqttMsg.fixedHeader.dup
        if(responseId):
            # This is a response to existing message
            if(status == 0):
                errorCode, errorMsg = None, None
                if(isinstance(body, dict)):
                    if(body.has_key("error_code")):
                        errorCode = body.get("error_code")
                    if(body.has_key("error_msg")):
                        errorMsg = body.get("error_msg")
                result = Result(None, 0, (errorCode, errorMsg))
            else:
                msg = Message(messageId, self.__clientId, body, qos, dup, replyTopic=replyTopic, instaMsg=self)
                result = Result(msg, 1)
            
            if(self.__sendMsgReplyHandlers.has_key(responseId)):
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
        ipAddress = self.__get_ip_address("wlan0")
        session = {'method':"wlan0", 'ip_address':ipAddress, 'antina_status': '', 'signal_strength': ''}
        self.publish(self.__sessionTopic, str(session), 1, 0)

    def __sendClientMetadata(self):
        imei = self.__getserial()
        metadata = {'imei': imei,'serial_number': imei, 'model': '',
                'firmware_version':'', 'manufacturer':''}
        self.publish(self.__metadataTopic, str(metadata), 1, 0)
        
    def __get_ip_address(self,ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(
            s.fileno(),
            0x8915,  # SIOCGIFADDR
            struct.pack('256s', ifname[:15])
        )[20:24])
        
    def __getserial(self):
        cpuserial = "0000000000000000"
        try:
            f = open('/proc/cpuinfo','r')
            for line in f:
                if line[0:6]=='Serial':
                    cpuserial = line[10:26]
            f.close()
        except:
            cpuserial = "ERROR00000000000"
        return cpuserial
                
    def __reboot(self):
        try:
            self.log(INSTAMSG_LOG_LEVEL_INFO,"Rebooting system.")
            os.system("reboot")
        except Exception, e:
            print str(e)
            
class Message:
    def __init__(self, messageId, topic, body, qos=INSTAMSG_QOS0, dup=0, replyTopic=None, instaMsg=None):
        self.__instaMsg = instaMsg
        self.__id = messageId
        self.__topic = topic
        self.__body = body
        self.__replyTopic = replyTopic
        self.__responseId = None
        self.__dup = dup
        self.__qos = qos
        
    def id(self):
        return self.__id
    
    def topic(self):
        return self.__topic
    
    def qos(self):
        return self.__qos
    
    def isDublicate(self):
        return self.__dup
    
    def body(self):
        return self.__body
    
    def replyTopic(self):
        return self.__replyTopic
        
    def reply(self, msg, dup=0, replyHandler=None, timeout=InstaMsg.INSTAMSG_RESULT_HANDLER_TIMEOUT):
        if(self.__instaMsg and self.__replyTopic):
            msgId = self.__instaMsg._generateMessageId()
            replyMsgJsonString = ('{"message_id": "%s", "response_id": "%s", "reply_to": "%s", "body": "%s", "status": 1}') % (msgId, self.__id, self.__topic, msg)
            self.__instaMsg._send(msgId, self.__replyTopic, replyMsgJsonString, self.__qos, dup, replyHandler, timeout)
    
    def fail(self, errorCode, errorMsg):
        if(self.__instaMsg and self.__replyTopic):
            msgId = self.__instaMsg._generateMessageId()
            failReplyMsgJsonString = ('{"message_id": "%s", "response_id": "%s", "reply_to": "%s", "body": {"error_code":%d, "error_msg":%s}, "status": 0}') % (msgId, self.__id, self.__topic, errorCode, errorMsg)
            self.__instaMsg._send(msgId, self.__replyTopic, failReplyMsgJsonString, self.__qos, 0, None, 0)
    
    def sendFile(self, fileName, resultHandler, timeout):
        pass
    
    def _sendMsgJsonString(self):
        return ('{"message_id": "%s", "reply_to": "%s", "body": "%s"}') % (self.__id, self.__replyTopic, self.__body)
    
    def toString(self):
        return ('[ id=%s, topic=%s, body=%s, qos=%s, dup=%s, replyTopic=%s]') % (str(self.__id), str(self.__topic), str(self.__body), str(self.__qos), str(self.__dup), str(self.__replyTopic))
    
    def __sendReply(self, msg, replyHandler):
        pass
    
class Result:
    def __init__(self, result, succeeded=1, cause=None):
        self.__result = result
        self.__succeeded = 1
        self.__cause = cause
        
    def result(self):
        return self.__result
    
    def failed(self):
        return not self.__succeeded
    
    def succeeded(self):
        return self.__succeeded
    
    def cause(self):
        return self.__cause

class InstaMsgError(Exception):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class InstaMsgSubError(InstaMsgError):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class InstaMsgUnSubError(InstaMsgError):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class InstaMsgPubError(InstaMsgError):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class InstaMsgSendError(InstaMsgError):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
####MqttClient ###############################################################################

class MqttClient:
    MQTT_PROTOCOL_VERSION = 3
    MQTT_PROTOCOL_NAME = "MQIsdp"
    MQTT_MAX_INT = 65535
    MQTT_RESULT_HANDLER_TIMEOUT = 10
    MQTT_MAX_RESULT_HANDLER_TIMEOUT = 500
    MAX_BYTES_MDM_READ = 511  # Telit MDM read limit
    MQTT_MAX_TOPIC_LEN = 32767
    MQTT_MAX_PAYLOAD_SIZE = 10000
    # Mqtt Message Types
    CONNECT = 0x10
    CONNACK = 0x20
    PUBLISH = 0x30
    PUBACK = 0x40
    PUBREC = 0x50
    PUBREL = 0x60
    PUBCOMP = 0x70
    SUBSCRIBE = 0x80
    SUBACK = 0x90
    UNSUBSCRIBE = 0xA0
    UNSUBACK = 0xB0
    PINGREQ = 0xC0
    PINGRESP = 0xD0
    DISCONNECT = 0xE0
    # CONNACK codes
    CONNECTION_ACCEPTED = 0x00
    CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION = 0X01
    CONNECTION_REFUSED_IDENTIFIER_REJECTED = 0x02
    CONNECTION_REFUSED_SERVER_UNAVAILABLE = 0x03
    CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD = 0x04
    CONNECTION_REFUSED_NOT_AUTHORIZED = 0x05;
    # QOS codes
    MQTT_QOS0 = 0
    MQTT_QOS1 = 1
    MQTT_QOS2 = 2

    def __init__(self, host, port, clientId, options={}):
        if(not clientId):
            raise ValueError('clientId cannot be null.')
        if(not host):
            raise ValueError('host cannot be null.')
        if(not port):
            raise ValueError('port cannot be null.')
        self.lock = RLock()
        self.host = host
        self.port = port
        self.clientId = clientId
        self.options = options
        self.options['clientId'] = clientId
        self.keepAliveTimer = self.options['keepAliveTimer']
        self.reconnectTimer = options['reconnectTimer']
        self.__logLevel = options.get('logLevel')
        self.__cleanSession = 1
        self.__sock = None
        self.__sockInit = 0
        self.__connected = 0
        self.__connecting = 0
        self.__disconnecting = 0
        self.__waitingReconnect = 0
        self.__nextConnTry = time.time()
        self.__lastPingReqTime = time.time()
        self.__lastPingRespTime = self.__lastPingReqTime
        self.__mqttMsgFactory = MqttMsgFactory()
        self.__mqttEncoder = MqttEncoder()
        self.__mqttDecoder = MqttDecoder()
        self.__messageId = 0
        self.__onDisconnectCallBack = None
        self.__onConnectCallBack = None
        self.__onMessageCallBack = None
        self.__onDebugMessageCallBack = None
        self.__msgIdInbox = []
        self.__resultHandlers = {}  # {handlerId:{time:122334,handler:replyHandler, timeout:10, timeOutMsg:"Timed out"}}
        self.__serverLogsTopic = "instamsg/" + clientId + "-" + self.options['username']+ "/logs";
        self.__size = 0
        
    def process(self):
        try:
            if(not self.__disconnecting):
                self.connect()
                if(self.__sockInit):
                    self.__receive()
                    if (self.__connected and (self.__lastPingReqTime + self.keepAliveTimer < time.time())):
                        if (self.__lastPingRespTime is None):
                            self.disconnect()
                        else: 
                            self.__sendPingReq()
                            self.__lastPingReqTime = time.time()
                            self.__lastPingRespTime = None
                self.__processHandlersTimeout()
#         except SocketTimeoutError:
#             pass
#         except SocketError, msg:
        except socket.error, msg:
            self.__resetInitSockNConnect()
            self.__log(INSTAMSG_LOG_LEVEL_DEBUG, "[MqttClientError, method = process][SocketError]:: %s" % (str(msg)))
        except:
            self.__log(INSTAMSG_LOG_LEVEL_ERROR, "[MqttClientError, method = process][Exception]:: %s %s" % (str(sys.exc_info()[0]), str(sys.exc_info()[1])))
    
    def connect(self):
        try:
            self.__initSock()
            if(self.__connecting is 0 and self.__sockInit):
                if(not self.__connected):
                    self.__connecting = 1
                    self.__log(INSTAMSG_LOG_LEVEL_INFO, '[MqttClient]:: Connecting to %s:%s' % (self.host, str(self.port)))   
                    fixedHeader = MqttFixedHeader(self.CONNECT, qos=0, dup=0, retain=0)
                    connectMsg = self.__mqttMsgFactory.message(fixedHeader, self.options, self.options)
                    encodedMsg = self.__mqttEncoder.ecode(connectMsg)
                    self.__sendall(encodedMsg)
#         except SocketTimeoutError:
#             pass
#         except SocketError, msg:
        except socket.timeout:
            self.__connecting = 0
            self.__log(INSTAMSG_LOG_LEVEL_DEBUG, "[MqttClientError, method = connect][SocketTimeoutError]:: Socket timed out")
        except socket.error, msg:
            self.__disconnecting = 1
            self.__resetInitSockNConnect()
            self.__log(INSTAMSG_LOG_LEVEL_DEBUG, "[MqttClientError, method = connect][SocketError]:: %s" % (str(msg)))
        except:
            self.__connecting = 0
            self.__log(INSTAMSG_LOG_LEVEL_ERROR, "[MqttClientError, method = connect][Exception]:: %s %s" % (str(sys.exc_info()[0]), str(sys.exc_info()[1])))
    
    def disconnect(self):
        try:
            try:
                self.__disconnecting = 1
                if(not self.__connecting  and not self.__waitingReconnect and self.__sockInit):
                    fixedHeader = MqttFixedHeader(self.DISCONNECT, qos=0, dup=0, retain=0)
                    disConnectMsg = self.__mqttMsgFactory.message(fixedHeader)
                    encodedMsg = self.__mqttEncoder.ecode(disConnectMsg)
                    self.__sendall(encodedMsg)
            except Exception, msg:
                self.__log(INSTAMSG_LOG_LEVEL_DEBUG, "[MqttClientError, method = __receive][%s]:: %s" % (msg.__class__.__name__ , str(msg)))
        finally:
            self.__closeSocket()
            self.__resetInitSockNConnect()
    
    def publish(self, topic, payload, qos=MQTT_QOS0, dup=0, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT, retain=0,logging=1):
        if(not self.__connected or self.__connecting  or self.__waitingReconnect):
            raise MqttClientError("Cannot publish message as not connected.")
        self.__validateTopic(topic)
        self.__validateQos(qos)
        self.__validateResultHandler(resultHandler)
        self.__validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(self.PUBLISH, qos, dup=0, retain=0)
        messageId = 0
        if(qos > self.MQTT_QOS0): messageId = self.__generateMessageId()
        variableHeader = {'messageId': messageId, 'topic': str(topic)}
        publishMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader, payload)
        encodedMsg = self.__mqttEncoder.ecode(publishMsg)
        self.__sendall(encodedMsg)
        if(topic != self.__serverLogsTopic):
            self.__log(INSTAMSG_LOG_LEVEL_DEBUG, '[MqttClient]:: sending message:%s' % publishMsg.toString())
        self.__validateResultHandler(resultHandler)
        if(qos == self.MQTT_QOS0 and resultHandler): 
            resultHandler(Result(None, 1))  # immediately return messageId 0 in case of qos 0
        elif (qos > self.MQTT_QOS0 and messageId and resultHandler): 
            timeOutMsg = 'Publishing message %s to topic %s with qos %d timed out.' % (payload, topic, qos)
            self.__resultHandlers[messageId] = {'time':time.time(), 'timeout': resultHandlerTimeout, 'handler':resultHandler, 'timeOutMsg':timeOutMsg}
        
    def subscribe(self, topic, qos, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT):
        if(not self.__connected or self.__connecting  or self.__waitingReconnect):
            raise MqttClientError("Cannot subscribe as not connected.")
        self.__validateTopic(topic)
        self.__validateQos(qos)
        self.__validateResultHandler(resultHandler)
        self.__validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(self.SUBSCRIBE, qos=1, dup=0, retain=0)
        messageId = self.__generateMessageId()
        variableHeader = {'messageId': messageId}
        subMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader, {'topic':topic, 'qos':qos})
        encodedMsg = self.__mqttEncoder.ecode(subMsg)
        self.__onDebugMessageCallBack(1, subMsg.toString())
        if(resultHandler):
            timeOutMsg = 'Subscribe to topic %s with qos %d timed out.' % (topic, qos)
            self.__resultHandlers[messageId] = {'time':time.time(), 'timeout': resultHandlerTimeout, 'handler':resultHandler, 'timeOutMsg':timeOutMsg}
        self.__sendall(encodedMsg)
                
    def unsubscribe(self, topics, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT):
        if(not self.__connected or self.__connecting  or self.__waitingReconnect):
            raise MqttClientError("Cannot unsubscribe as not connected.")
        self.__validateResultHandler(resultHandler)
        self.__validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(self.UNSUBSCRIBE, qos=1, dup=0, retain=0)
        messageId = self.__generateMessageId()
        variableHeader = {'messageId': messageId}
        if(isinstance(topics, str)):
            topics = [topics]
        if(isinstance(topics, list)):
            for topic in topics:
                self.__validateTopic(topic)
                unsubMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader, topics)
                encodedMsg = self.__mqttEncoder.ecode(unsubMsg)
                self.__onDebugMessageCallBack(1,'sending unsubMsg : ' + unsubMsg.toString())
                if(resultHandler):
                    timeOutMsg = 'Unsubscribe to topics %s timed out.' % str(topics)
                    self.__resultHandlers[messageId] = {'time':time.time(), 'timeout': resultHandlerTimeout, 'handler':resultHandler, 'timeOutMsg':timeOutMsg}
                self.__sendall(encodedMsg)
                return messageId
        else:   raise TypeError('Topics should be an instance of string or list.') 
    
    def onConnect(self, callback):
        if(callable(callback)):
            self.__onConnectCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.')
    
    def connected(self):
        return self.__connected  and not self.__disconnecting
        
    def onDisconnect(self, callback):
        if(callable(callback)):
            self.__onDisconnectCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.')
        
    def onDebugMessage(self, callback):
        if(callable(callback)):
            self.__onDebugMessageCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.') 
    
    def onMessage(self, callback):
        if(callable(callback)):
            self.__onMessageCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.') 
        
    def __validateTopic(self, topic):
        if(topic):
            pass
        else: raise ValueError('Topics cannot be Null or empty.')
        if (len(topic) < self.MQTT_MAX_TOPIC_LEN + 1):
            pass
        else:
            raise ValueError('Topic length cannot be more than %d' % self.MQTT_MAX_TOPIC_LEN)
        
    def __validateQos(self, qos):
        if(not isinstance(qos, int) or qos < self.MQTT_QOS0 or qos > self.MQTT_QOS2):
            raise ValueError('Qos should be a between %d and %d.' % (self.MQTT_QOS0, self.MQTT_QOS2)) 
        
    def __validateRetain(self, retain):
        if (not isinstance(retain, int) or retain != 0 or retain != 1):
            raise ValueError('Retain can only be integer 0 or 1')
        
    def __validateTimeout(self, timeout):
        if (not isinstance(timeout, int) or timeout < 0 or timeout > self.MQTT_MAX_RESULT_HANDLER_TIMEOUT):
            raise ValueError('Timeout can only be integer between 0 and %d.' % self.MQTT_MAX_RESULT_HANDLER_TIMEOUT)
        
    def __validateResultHandler(self, resultHandler):
        if(resultHandler is not None and not callable(resultHandler)):            
            raise ValueError('Result Handler should be a callable object.') 
            
    def __log(self, level, msg):
        if(level <= self.__logLevel):
            if(self.__onDebugMessageCallBack):
                self.__onDebugMessageCallBack(level, msg)

    def __sendall(self, data):
        try:
            if(data):
                self.lock.acquire()
                try:
                    self.__sock.sendall(data)
                except socket.error, msg:
                    self.__disconnecting = 1
                    self.__resetInitSockNConnect()
                    raise socket.error(str("Socket error in send: %s. Connection reset." % (str(msg))))
        finally:
            if(data):
                self.lock.release()
            
            
    def __receive(self):
        try:
            self.lock.acquire()
            try:
                data = self.__sock.recv(self.MAX_BYTES_MDM_READ)
                if data: 
                    mqttMsg = self.__mqttDecoder.decode(data)
                    self.__size = self.__size + sys.getsizeof(data)
                    if(self.__size > 1000):
                        self.__sendReceivedDataSize()                  
                else:
                    mqttMsg = None
                if (mqttMsg):
                    if(mqttMsg.fixedHeader.messageType == self.PUBLISH and mqttMsg.topic == self.__serverLogsTopic):
                        pass
                    else:
                        self.__log(INSTAMSG_LOG_LEVEL_INFO, '[MqttClient]:: Received message:%s' % mqttMsg.toString())
                    self.__handleMqttMessage(mqttMsg) 
            except MqttDecoderError, msg:
                self.__log(INSTAMSG_LOG_LEVEL_DEBUG, "[MqttClientError, method = __receive][%s]:: %s" % (msg.__class__.__name__ , str(msg)))
            except socket.timeout:
                pass
            except (MqttFrameError, socket.error), msg:
                if 'timed out' in msg.message.lower():
                    #Hack as ssl library does not throw timeout error
                    pass
                else:
                    self.__resetInitSockNConnect()
                    self.__log(INSTAMSG_LOG_LEVEL_DEBUG, "[MqttClientError, method = __receive][%s]:: %s" % (msg.__class__.__name__ , str(msg)))
        finally:
            self.lock.release() 
           
    def __handleMqttMessage(self, mqttMessage):
        self.__lastPingRespTime = time.time()
        msgType = mqttMessage.fixedHeader.messageType
        if msgType == self.CONNACK:
            self.__handleConnAckMsg(mqttMessage)
        elif msgType == self.PUBLISH:
            self.__handlePublishMsg(mqttMessage)
        elif msgType == self.SUBACK:
            self.__handleSubAck(mqttMessage)
        elif msgType == self.UNSUBACK:
            self.__handleUnSubAck(mqttMessage)
        elif msgType == self.PUBACK:
            self.__sendPubAckMsg(mqttMessage)
        elif msgType == self.PUBREC:
            self.__handlePubRecMsg(mqttMessage)
        elif msgType == self.PUBCOMP:
            self.__onPublish(mqttMessage)
        elif msgType == self.PUBREL:
            self.__handlePubRelMsg(mqttMessage)
        elif msgType == self.PINGRESP:
            self.__lastPingRespTime = time.time()
        elif msgType in [self.CONNECT, self.SUBSCRIBE, self.UNSUBSCRIBE, self.PINGREQ]:
            pass  # Client will not receive these messages
        else:
            raise MqttEncoderError('MqttEncoder: Unknown message type.') 
    
    def __handleSubAck(self, mqttMessage):
        resultHandler = self.__resultHandlers.get(mqttMessage.messageId).get('handler')
        if(resultHandler):
            resultHandler(Result(mqttMessage, 1))
            del self.__resultHandlers[mqttMessage.messageId]
    
    def __handleUnSubAck(self, mqttMessage):
        resultHandler = self.__resultHandlers.get(mqttMessage.messageId).get('handler')
        if(resultHandler):
            resultHandler(Result(mqttMessage, 1))
            del self.__resultHandlers[mqttMessage.messageId]
    
    def __onPublish(self, mqttMessage):
        resultHandler = self.__resultHandlers.get(mqttMessage.messageId).get('handler')
        if(resultHandler):
            resultHandler(Result(mqttMessage, 1))
            del self.__resultHandlers[mqttMessage.messageId]
    
    def __handleConnAckMsg(self, mqttMessage):
        self.__connecting = 0
        connectReturnCode = mqttMessage.connectReturnCode
        if(connectReturnCode == self.CONNECTION_ACCEPTED):
            self.__connected = 1
            self.__log(INSTAMSG_LOG_LEVEL_INFO, '[MqttClient]:: Connected to %s:%s' % (self.host, str(self.port)))  
            if(self.__onConnectCallBack): self.__onConnectCallBack(self)  
        elif(connectReturnCode == self.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION):
            self.__log(INSTAMSG_LOG_LEVEL_DEBUG, '[MqttClient]:: Connection refused unacceptable mqtt protocol version.')
        elif(connectReturnCode == self.CONNECTION_REFUSED_IDENTIFIER_REJECTED):
            self.__log(INSTAMSG_LOG_LEVEL_DEBUG, '[MqttClient]:: Connection refused client identifier rejected.')  
        elif(connectReturnCode == self.CONNECTION_REFUSED_SERVER_UNAVAILABLE):  
            self.__log(INSTAMSG_LOG_LEVEL_DEBUG, '[MqttClient]:: Connection refused server unavailable.')
        elif(connectReturnCode == self.CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD):  
            self.__log(INSTAMSG_LOG_LEVEL_DEBUG, '[MqttClient]:: Connection refused bad username or password.')
        elif(connectReturnCode == self.CONNECTION_REFUSED_NOT_AUTHORIZED):  
            self.__log(INSTAMSG_LOG_LEVEL_DEBUG, '[MqttClient]:: Connection refused not authorized.')
    
    def __handlePublishMsg(self, mqttMessage):
        if(mqttMessage.fixedHeader.qos > self.MQTT_QOS1): 
            if(mqttMessage.messageId not in self.__msgIdInbox):
                self.__msgIdInbox.append(mqttMessage.messageId)
        if(self.__onMessageCallBack):
            self.__onMessageCallBack(mqttMessage)

    def __sendPubAckMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(self.PUBACK)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubAckMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self.__mqttEncoder.ecode(pubAckMsg)
        self.__sendall(encodedMsg)
            
    def __handlePubRelMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(self.PUBCOMP)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubComMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self.__mqttEncoder.ecode(pubComMsg)
        self.__sendall(encodedMsg)
        if(mqttMessage.messageId  in self.__msgIdInbox):
            self.__msgIdInbox.remove(mqttMessage.messageId)
    
    def __handlePubRecMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(self.PUBREL,1)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubRelMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self.__mqttEncoder.ecode(pubRelMsg)
        self.__sendall(encodedMsg)
        
    def __sendReceivedDataSize(self):
        self.publish("instamsg/client/receiveddata", str({'received_data': self.__size}))
        self.__size = 0
    
    def __resetInitSockNConnect(self):
        if(self.__sockInit):
            self.__log(INSTAMSG_LOG_LEVEL_INFO, '[MqttClient]:: Resetting connection due to socket error...')
            self.__closeSocket()
            if(self.__onDisconnectCallBack): self.__onDisconnectCallBack()
        self.__sockInit = 0
        self.__connected = 0
        self.__connecting = 0
        self.__disconnecting = 0
        self.__lastPingReqTime = time.time()
        self.__lastPingRespTime = self.__lastPingReqTime
        
    
    def __initSock(self):
        t = time.time()
#         if (self.__sockInit is 0 and self.__nextConnTry - t > 0): raise SocketError('Last connection failed. Waiting before retry.')
        waitFor = self.__nextConnTry - t
        if (self.__sockInit is 0 and waitFor > 0): 
            if(not self.__waitingReconnect):
                self.__waitingReconnect = 1
                self.__log(INSTAMSG_LOG_LEVEL_DEBUG, '[MqttClient]:: Last connection failed. Waiting  for %d seconds before retry...' % int(waitFor))
        if (self.__sockInit is 0 and waitFor <= 0):
            self.__nextConnTry = t + self.reconnectTimer
            if(self.__sock is not None):
                self.__closeSocket()
                self.__log(INSTAMSG_LOG_LEVEL_INFO, '[MqttClient]:: Opening socket to %s:%s' % (self.host, str(self.port)))
#             self.__sock = Socket(10, self.keepAlive)
            self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            if(self.port == InstaMsg.INSTAMSG_PORT_SSL):
                self.__sock = ssl.wrap_socket(self.__sock, cert_reqs=ssl.CERT_NONE)
                self.__sock.settimeout(10)
                self.__sock.connect((self.host, self.port))
                commonName = self.__getCommonNameFromCertificate()
                domain = self.host.split(".")[-2:]
                domain = ".".join(domain)
                if(commonName == domain):
                    self.__sockInit = 1
                    self.__waitingReconnect = 0
                    self.__log(INSTAMSG_LOG_LEVEL_INFO, '[MqttClient]:: Socket opened to %s:%s' % (self.host, str(self.port)))
                else:
                    self.__log(INSTAMSG_LOG_LEVEL_ERROR, '[MqttClient]:: Ssl certificate error. Host %s does not match host %s provide in certificate.' % (self.host, commonName))  
            else:
                self.__sock.settimeout(10)
                self.__sock.connect((self.host, self.port))
                self.__sockInit = 1
                self.__waitingReconnect = 0
                self.__log(INSTAMSG_LOG_LEVEL_INFO, '[MqttClient]:: Socket opened to %s:%s' % (self.host, str(self.port))) 
            
    def __getCommonNameFromCertificate(self):
        certDer = self.__sock.getpeercert(binary_form=True)
        if(certDer is not None):
            x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_ASN1, certDer)
            return x509.get_subject().commonName
        return None
    
    def __closeSocket(self):
        try:
            self.__log(INSTAMSG_LOG_LEVEL_INFO, '[MqttClient]:: Closing socket...')
            if(self.__sock):
                self.__sock.close()
                self.__sock = None
        except:
            pass 
    
    def __generateMessageId(self): 
        if self.__messageId == self.MQTT_MAX_INT:
            self.__messageId = 0
        self.__messageId = self.__messageId + 1
        return self.__messageId
    
    def __processHandlersTimeout(self):
        for key, value in self.__resultHandlers.items():
            if((time.time() - value['time']) >= value['timeout']):
                resultHandler = value['handler']
                if(resultHandler):
                    timeOutMsg = value['timeOutMsg']
                    resultHandler(Result(None, 0, (INSTAMSG_ERROR_TIMEOUT, timeOutMsg)))
                    value['handler'] = None
                del self.__resultHandlers[key]
                
    def __sendPingReq(self):
        fixedHeader = MqttFixedHeader(self.PINGREQ)
        pingReqMsg = self.__mqttMsgFactory.message(fixedHeader)
        encodedMsg = self.__mqttEncoder.ecode(pingReqMsg)
        self.__sendall(encodedMsg)
    
####Mqtt Codec ###############################################################################

class MqttDecoder:
    READING_FIXED_HEADER_FIRST = 0
    READING_FIXED_HEADER_REMAINING = 1
    READING_VARIABLE_HEADER = 2
    READING_PAYLOAD = 3
    DISCARDING_MESSAGE = 4
    MESSAGE_READY = 5
    BAD = 6
    
    def __init__(self):
        self.__data = ''
        self.__init()
        self.__msgFactory = MqttMsgFactory()
        
    def __state(self):
        return self.__state
    
    def decode(self, data):
        if(data):
            self.__data = self.__data + data
            if(self.__state == self.READING_FIXED_HEADER_FIRST):
                self.__decodeFixedHeaderFirstByte(self.__getByteStr())
                self.__state = self.READING_FIXED_HEADER_REMAINING
            if(self.__state == self.READING_FIXED_HEADER_REMAINING):
                self.__decodeFixedHeaderRemainingLength()
                if (self.__fixedHeader.messageType == MqttClient.PUBLISH and not self.__variableHeader):
                    self.__initPubVariableHeader()
            if(self.__state == self.READING_VARIABLE_HEADER):
                self.__decodeVariableHeader()
            if(self.__state == self.READING_PAYLOAD):
                bytesRemaining = self.__remainingLength - (self.__bytesConsumedCounter - self.__remainingLengthCounter - 1)
                self.__decodePayload(bytesRemaining)
            if(self.__state == self.DISCARDING_MESSAGE):
                bytesLeftToDiscard = self.__remainingLength - self.__bytesDiscardedCounter
                if (bytesLeftToDiscard <= len(self.__data)):
                    bytesToDiscard = bytesLeftToDiscard
                else: bytesToDiscard = len(self.__data)
                self.__bytesDiscardedCounter = self.__bytesDiscardedCounter + bytesToDiscard
                self.__data = self.__data[0:(bytesToDiscard - 1)] 
                if(self.__bytesDiscardedCounter == self.__remainingLength):
                    e = self.__error
                    self.__init()
                    raise MqttDecoderError(e) 
            if(self.__state == self.MESSAGE_READY):
                # returns a tuple of (mqttMessage, dataRemaining)
                mqttMsg = self.__msgFactory.message(self.__fixedHeader, self.__variableHeader, self.__payload)
                self.__init()
                return mqttMsg
            if(self.__state == self.BAD):
                # do not decode until disconnection.
                raise MqttFrameError(self.__error)  
        return None 
            
    def __decodeFixedHeaderFirstByte(self, byteStr):
        byte = ord(byteStr)
        self.__fixedHeader.messageType = (byte & 0xF0)
        self.__fixedHeader.dup = (byte & 0x08) >> 3
        self.__fixedHeader.qos = (byte & 0x06) >> 1
        self.__fixedHeader.retain = (byte & 0x01)
    
    def __decodeFixedHeaderRemainingLength(self):
            while (1 and self.__data):
                byte = ord(self.__getByteStr())
                self.__remainingLength += (byte & 127) * self.__multiplier
                self.__multiplier *= 128
                self.__remainingLengthCounter = self.__remainingLengthCounter + 1
                if(self.__remainingLengthCounter > 4):
                    self.__state = self.BAD
                    self.__error = ('MqttDecoder: Error in decoding remaining length in message fixed header.') 
                    break
                if((byte & 128) == 0):
                    self.__state = self.READING_VARIABLE_HEADER
                    self.__fixedHeader.remainingLength = self.__remainingLength
                    break
                
    def __initPubVariableHeader(self):
        self.__variableHeader['topicLength'] = None
        self.__variableHeader['messageId'] = None
        self.__variableHeader['topic'] = None
        

    def __decodeVariableHeader(self):  
        if self.__fixedHeader.messageType in [MqttClient.CONNECT, MqttClient.SUBSCRIBE, MqttClient.UNSUBSCRIBE, MqttClient.PINGREQ]:
            self.__state = self.DISCARDING_MESSAGE
            self.__error = ('MqttDecoder: Client cannot receive CONNECT, SUBSCRIBE, UNSUBSCRIBE, PINGREQ message type.') 
        elif self.__fixedHeader.messageType == MqttClient.CONNACK:
            if(self.__fixedHeader.remainingLength != 2):
                self.__state = self.BAD
                self.__error = ('MqttDecoder: Mqtt CONNACK message should have remaining length 2 received %s.' % self.__fixedHeader.remainingLength) 
            elif(len(self.__data) < 2):
                pass  # let for more bytes come
            else:
                self.__getByteStr()  # discard reserved byte
                self.__variableHeader['connectReturnCode'] = ord(self.__getByteStr())
                self.__state = self.MESSAGE_READY
        elif self.__fixedHeader.messageType == MqttClient.SUBACK:
            messageId = self.__decodeMsbLsb()
            if(messageId is not None):
                self.__variableHeader['messageId'] = messageId
                self.__state = self.READING_PAYLOAD
        elif self.__fixedHeader.messageType in [MqttClient.UNSUBACK, MqttClient.PUBACK, MqttClient.PUBREC, MqttClient.PUBCOMP, MqttClient.PUBREL]:
            messageId = self.__decodeMsbLsb()
            if(messageId is not None):
                self.__variableHeader['messageId'] = messageId
                self.__state = self.MESSAGE_READY
        elif self.__fixedHeader.messageType == MqttClient.PUBLISH:
            if(self.__variableHeader['topic'] is None):
                self.__decodeTopic()
            if (self.__fixedHeader.qos > MqttClient.MQTT_QOS0 and self.__variableHeader['topic'] is not None and self.__variableHeader['messageId'] is None):
                self.__variableHeader['messageId'] = self.__decodeMsbLsb()
            if (self.__variableHeader['topic'] is not None and (self.__fixedHeader.qos == MqttClient.MQTT_QOS0 or self.__variableHeader['messageId'] is not None)):
                self.__state = self.READING_PAYLOAD
        elif self.__fixedHeader.messageType in [MqttClient.PINGRESP, MqttClient.DISCONNECT]:
            self.__mqttMsg = self.__msgFactory.message(self.__fixedHeader)
            self.__state = self.MESSAGE_READY
        else:
            self.__state = self.DISCARDING_MESSAGE
            self.__error = ('MqttDecoder: Unrecognised message type.') 
            
    def __decodePayload(self, bytesRemaining):
        paloadBytes = self.__getNBytesStr(bytesRemaining)
        if(paloadBytes is not None):
            if self.__fixedHeader.messageType == MqttClient.SUBACK:
                grantedQos = []
                numberOfBytesConsumed = 0
                while (numberOfBytesConsumed < bytesRemaining):
                    qos = int(ord(paloadBytes[numberOfBytesConsumed]) & 0x03)
                    numberOfBytesConsumed = numberOfBytesConsumed + 1
                    grantedQos.append(qos)
                self.__payload = grantedQos
                self.__state = self.MESSAGE_READY
            elif self.__fixedHeader.messageType == MqttClient.PUBLISH:
                self.__payload = paloadBytes
                self.__state = self.MESSAGE_READY
    
    def __decodeTopic(self):
        stringLength = self.__variableHeader['topicLength']
        if(stringLength is None):
            stringLength = self.__decodeMsbLsb()
            self.__variableHeader['topicLength'] = stringLength
        if (self.__data and stringLength and (len(self.__data) < stringLength)):
            return None  # wait for more bytes
        else:
            self.__variableHeader['topic'] = self.__getNBytesStr(stringLength)
    
    def __decodeMsbLsb(self):
        if(len(self.__data) < 2):
            return None  # wait for 2 bytes
        else:
            msb = self.__getByteStr()
            lsb = self.__getByteStr()
            intMsbLsb = ord(msb) << 8 | ord(lsb)
        if (intMsbLsb < 0 or intMsbLsb > MqttClient.MQTT_MAX_INT):
            return -1
        else:
            return intMsbLsb
        
    
    def __getByteStr(self):
        return self.__getNBytesStr(1)
    
    def __getNBytesStr(self, n):
        # gets n or less bytes
        nBytes = self.__data[0:n]
        self.__data = self.__data[n:len(self.__data)]
        self.__bytesConsumedCounter = self.__bytesConsumedCounter + n
        return nBytes
    
    def __init(self):   
        self.__state = self.READING_FIXED_HEADER_FIRST
        self.__remainingLength = 0
        self.__multiplier = 1
        self.__remainingLengthCounter = 0
        self.__bytesConsumedCounter = 0
        self.__payloadCounter = 0
        self.__fixedHeader = MqttFixedHeader()
        self.__variableHeader = {}
        self.__payload = None
        self.__mqttMsg = None
        self.__bytesDiscardedCounter = 0 
        self.__error = 'MqttDecoder: Unrecognized __error'

class MqttEncoder:
    def __init__(self):
        pass
    
    def ecode(self, mqttMessage):
        msgType = mqttMessage.fixedHeader.messageType
        if msgType == MqttClient.CONNECT:
            return self.__encodeConnectMsg(mqttMessage) 
        elif msgType == MqttClient.CONNACK:
            return self.__encodeConnAckMsg(mqttMessage)
        elif msgType == MqttClient.PUBLISH:
            return self.__encodePublishMsg(mqttMessage)
        elif msgType == MqttClient.SUBSCRIBE:
            return self.__encodeSubscribeMsg(mqttMessage)
        elif msgType == MqttClient.UNSUBSCRIBE:
            return self.__encodeUnsubscribeMsg(mqttMessage)
        elif msgType == MqttClient.SUBACK:
            return self.__encodeSubAckMsg(mqttMessage)
        elif msgType in [MqttClient.UNSUBACK, MqttClient.PUBACK, MqttClient.PUBREC, MqttClient.PUBCOMP, MqttClient.PUBREL]:
            return self.__encodeFixedHeaderAndMessageIdOnlyMsg(mqttMessage)
        elif msgType in [MqttClient.PINGREQ, MqttClient.PINGRESP, MqttClient.DISCONNECT]:
            return self.__encodeFixedHeaderOnlyMsg(mqttMessage)
        else:
            raise MqttEncoderError('MqttEncoder: Unknown message type.') 
    
    def __encodeConnectMsg(self, mqttConnectMessage):
        if(isinstance(mqttConnectMessage, MqttConnectMsg)):
            variableHeaderSize = 12
            fixedHeader = mqttConnectMessage.fixedHeader
            # Encode Payload
            clientId = self.__encodeStringUtf8(mqttConnectMessage.clientId)
            if(not self.__isValidClientId(clientId)):
                raise ValueError("MqttEncoder: invalid clientId: " + clientId + " should be less than 23 chars in length.")
            encodedPayload = self.__encodeIntShort(len(clientId)) + clientId
            if(mqttConnectMessage.isWillFlag):
                encodedPayload = encodedPayload + self.__encodeIntShort(len(mqttConnectMessage.willTopic)) + self.__encodeStringUtf8(mqttConnectMessage.willTopic)
                encodedPayload = encodedPayload + self.__encodeIntShort(len(mqttConnectMessage.willMessage)) + self.__encodeStringUtf8(mqttConnectMessage.willMessage)
            if(mqttConnectMessage.hasUserName):
                encodedPayload = encodedPayload + self.__encodeIntShort(len(mqttConnectMessage.username)) + self.__encodeStringUtf8(mqttConnectMessage.username)
            if(mqttConnectMessage.hasPassword):
                encodedPayload = encodedPayload + self.__encodeIntShort(len(mqttConnectMessage.password)) + self.__encodeStringUtf8(mqttConnectMessage.password)
            # Encode Variable Header
            connectFlagsByte = 0;
            if (mqttConnectMessage.hasUserName): 
                connectFlagsByte |= 0x80
            if (mqttConnectMessage.hasPassword):
                connectFlagsByte |= 0x40
            if (mqttConnectMessage.isWillRetain):
                connectFlagsByte |= 0x20
            connectFlagsByte |= (mqttConnectMessage.willQos & 0x03) << 3
            if (mqttConnectMessage.isWillFlag):
                connectFlagsByte |= 0x04
            if (mqttConnectMessage.isCleanSession):
                connectFlagsByte |= 0x02;
            encodedVariableHeader = self.__encodeIntShort(len(mqttConnectMessage.protocolName)) + mqttConnectMessage.protocolName + chr(mqttConnectMessage.version) + chr(connectFlagsByte) + self.__encodeIntShort(mqttConnectMessage.keepAliveTimer)
            return self.__encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload) + encodedVariableHeader + encodedPayload
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttConnectMsg.__name__, mqttConnectMessage.__class__.__name__)) 
            
    def __encodeConnAckMsg(self, mqttConnAckMsg):
        if(isinstance(mqttConnAckMsg, MqttConnAckMsg)):
            fixedHeader = mqttConnAckMsg.fixedHeader
            encodedVariableHeader = mqttConnAckMsg.connectReturnCode
            return self.__encodeFixedHeader(fixedHeader, 2, None) + encodedVariableHeader
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttConnAckMsg.__name__, mqttConnAckMsg.__class__.__name__)) 

    def __encodePublishMsg(self, mqttPublishMsg):
        if(isinstance(mqttPublishMsg, MqttPublishMsg)):
            fixedHeader = mqttPublishMsg.fixedHeader
            topic = mqttPublishMsg.topic
            variableHeaderSize = 2 + len(topic) 
            if(fixedHeader.qos > 0):
                variableHeaderSize = variableHeaderSize + 2 
            encodedPayload = mqttPublishMsg.payload
            # Encode Variable Header
            encodedVariableHeader = self.__encodeIntShort(len(topic)) + self.__encodeStringUtf8(topic)
            if (fixedHeader.qos > 0): 
                encodedVariableHeader = encodedVariableHeader + self.__encodeIntShort(mqttPublishMsg.messageId)
            return self.__encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload) + encodedVariableHeader + str(encodedPayload)
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPublishMsg.__name__, mqttPublishMsg.__class__.__name__)) 
    
    def __encodeSubscribeMsg(self, mqttSubscribeMsg):
        if(isinstance(mqttSubscribeMsg, MqttSubscribeMsg)):
            fixedHeader = mqttSubscribeMsg.fixedHeader
            variableHeaderSize = 2
            # Encode Payload
            encodedPayload = ''
            topic = mqttSubscribeMsg.payload.get('topic')
            qos = mqttSubscribeMsg.payload.get('qos')
            encodedPayload = encodedPayload + self.__encodeIntShort(len(topic)) + self.__encodeStringUtf8(topic) + str(qos)
            # Encode Variable Header
            encodedVariableHeader = self.__encodeIntShort(mqttSubscribeMsg.messageId)
            return self.__encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload) + encodedVariableHeader + encodedPayload
                
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttSubscribeMsg.__name__, mqttSubscribeMsg.__class__.__name__))
    
    def __encodeUnsubscribeMsg(self, mqttUnsubscribeMsg):
        if(isinstance(mqttUnsubscribeMsg, MqttUnsubscribeMsg)):
            fixedHeader = mqttUnsubscribeMsg.fixedHeader
            variableHeaderSize = 2
            # Encode Payload
            encodedPayload = ''
            for topic in mqttUnsubscribeMsg.payload:
                encodedPayload = encodedPayload + self.__encodeIntShort(len(topic)) + self.__encodeStringUtf8(topic)
            # Encode Variable Header
            encodedVariableHeader = self.__encodeIntShort(mqttUnsubscribeMsg.messageId)
            return self.__encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload) + encodedVariableHeader + encodedPayload
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttUnsubscribeMsg.__name__, mqttUnsubscribeMsg.__class__.__name__))
    
    def __encodeSubAckMsg(self, mqttSubAckMsg):
        if(isinstance(mqttSubAckMsg, MqttSubAckMsg)):
            fixedHeader = mqttSubAckMsg.fixedHeader
            variableHeaderSize = 2
            # Encode Payload
            encodedPayload = ''
            for qos in mqttSubAckMsg.payload:
                encodedPayload = encodedPayload + str(qos)
            # Encode Variable Header
            encodedVariableHeader = self.__encodeIntShort(mqttSubAckMsg.messageId)
            return self.__encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload) + encodedVariableHeader + encodedPayload
            
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttSubAckMsg.__name__, mqttSubAckMsg.__class__.__name__))
    
    def __encodeFixedHeaderAndMessageIdOnlyMsg(self, mqttMessage):
        msgType = mqttMessage.fixedHeader.messageType
        if(isinstance(mqttMessage, MqttUnsubscribeMsg) or isinstance(mqttMessage, MqttPubAckMsg) or isinstance(mqttMessage, MqttPubRecMsg) or isinstance(mqttMessage, MqttPubCompMsg) or isinstance(mqttMessage, MqttPubRelMsg)):
            fixedHeader = mqttMessage.fixedHeader
            variableHeaderSize = 2
            # Encode Variable Header
            encodedVariableHeader = self.__encodeIntShort(mqttMessage.messageId)
            return self.__encodeFixedHeader(fixedHeader, variableHeaderSize, None) + encodedVariableHeader
        else:
            if msgType == MqttClient.UNSUBACK: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttUnsubAckMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == MqttClient.PUBACK: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubAckMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == MqttClient.PUBREC: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubRecMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == MqttClient.PUBCOMP: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubCompMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == MqttClient.PUBREL: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubRelMsg.__name__, mqttMessage.__class__.__name__))
    
    def __encodeFixedHeaderOnlyMsg(self, mqttMessage):
        msgType = mqttMessage.fixedHeader.messageType
        if(isinstance(mqttMessage, MqttPingReqMsg) or isinstance(mqttMessage, MqttPingRespMsg) or isinstance(mqttMessage, MqttDisconnetMsg)):
            return self.__encodeFixedHeader(mqttMessage.fixedHeader, 0, None)
        else:
            if msgType == MqttClient.PINGREQ: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPingReqMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == MqttClient.PINGRESP: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPingRespMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == MqttClient.DISCONNECT: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttDisconnetMsg.__name__, mqttMessage.__class__.__name__))
    
    def __encodeFixedHeader(self, fixedHeader, variableHeaderSize, encodedPayload):
        if encodedPayload is None:
            length = 0
        else: length = len(encodedPayload)
        encodedRemainingLength = self.__encodeRemainingLength(variableHeaderSize + length)
        return chr(self.__getFixedHeaderFirstByte(fixedHeader)) + encodedRemainingLength
    
    def __getFixedHeaderFirstByte(self, fixedHeader):
        firstByte = fixedHeader.messageType
        if (fixedHeader.dup):
            firstByte |= 0x08;
        firstByte |= fixedHeader.qos << 1;
        if (fixedHeader.retain):
            firstByte |= 0x01;
        return firstByte;
    
    def __encodeRemainingLength(self, num):
        remainingLength = ''
        while 1:
            digit = num % 128
            num /= 128
            if (num > 0):
                digit |= 0x80
            remainingLength += chr(digit) 
            if(num == 0):
                    break
        return  remainingLength   
    
    def __encodeIntShort(self, number):  
        return chr(number / 256) + chr(number % 256)
    
    def __encodeStringUtf8(self, s):
        return str(s)
    
    def __isValidClientId(self, clientId):   
        if (clientId is None):
            return 0
        length = len(clientId)
        return length >= 1 and length <= 23
                     
        
class MqttFixedHeader:
    def __init__(self, messageType=None, qos=0, dup=0, retain=0, remainingLength=0):
        self.messageType = messageType or None
        self.dup = dup or 0
        self.qos = qos or 0
        self.retain = retain or 0
        self.remainingLength = remainingLength or 0
    
    def toString(self):
        return 'fixedHeader=[messageType=%s, dup=%d, qos=%d, retain=%d, remainingLength=%d]' % (str(self.messageType), self.dup, self.qos, self.retain, self.remainingLength)
        
class MqttMsg:
    def __init__(self, fixedHeader, variableHeader=None, payload=None):
        self.fixedHeader = fixedHeader
        self.variableHeader = variableHeader
        self.payload = payload
        
    def toString(self):
        return '%s[[%s] [variableHeader= %s] [payload= %s]]' % (self.__class__.__name__, self.fixedHeader.toString(), str(self.variableHeader), str(self.payload))
        

class MqttConnectMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader, payload):
        MqttMsg.__init__(self, fixedHeader, variableHeader, payload)
        self.fixedHeader = fixedHeader
        self.protocolName = MqttClient.MQTT_PROTOCOL_NAME
        self.version = MqttClient.MQTT_PROTOCOL_VERSION
        self.hasUserName = variableHeader.get('hasUserName')
        self.hasPassword = variableHeader.get('hasPassword')
        self.clientId = payload.get('clientId')
        self.username = payload.get('username')
        self.password = payload.get('password')
        self.isWillRetain = variableHeader.get('isWillRetain')
        self.willQos = variableHeader.get('willQos')
        self.isWillFlag = variableHeader.get('isWillFlag')
        self.isCleanSession = variableHeader.get('isCleanSession')
        self.keepAliveTimer = variableHeader.get('keepAliveTimer')
        self.willTopic = payload.get('willTopic')
        self.willMessage = payload.get('willMessage')
        
class MqttConnAckMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader):
        MqttMsg.__init__(self, fixedHeader, variableHeader)
        self.fixedHeader = fixedHeader
        self.__variableHeader = variableHeader
        self.connectReturnCode = variableHeader.get('connectReturnCode')
        self.payload = None
        
class MqttPingReqMsg(MqttMsg):
    def __init__(self, fixedHeader):
        MqttMsg.__init__(self, fixedHeader)
        self.fixedHeader = fixedHeader
        
class MqttPingRespMsg(MqttMsg):
    def __init__(self, fixedHeader):
        MqttMsg.__init__(self, fixedHeader)
        self.fixedHeader = fixedHeader
        
class MqttDisconnetMsg(MqttMsg):
    def __init__(self, fixedHeader):
        MqttMsg.__init__(self, fixedHeader)
        self.fixedHeader = fixedHeader
        
class MqttPubAckMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader):
        MqttMsg.__init__(self, fixedHeader, variableHeader)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        
class MqttPubRecMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader):
        MqttMsg.__init__(self, fixedHeader, variableHeader)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        
class MqttPubRelMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader):
        MqttMsg.__init__(self, fixedHeader, variableHeader)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')

class MqttPubCompMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader):
        MqttMsg.__init__(self, fixedHeader, variableHeader)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')

class MqttPublishMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader, payload):
        MqttMsg.__init__(self, fixedHeader, variableHeader, payload)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        self.topic = variableHeader.get('topic')
        # __payload bytes
        self.payload = payload

class MqttSubscribeMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader, payload=[]):
        MqttMsg.__init__(self, fixedHeader, variableHeader, payload)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        # __payload = [{"topic":"a/b","qos":1}]
        self.payload = payload

class MqttSubAckMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader, payload=[]):
        MqttMsg.__init__(self, fixedHeader, variableHeader, payload)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        # __payload = [0,1,2]
        self.payload = payload

class MqttUnsubscribeMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader, payload=[]):
        MqttMsg.__init__(self, fixedHeader, variableHeader, payload)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        # __payload = [topic0,topic1,topic2]
        self.payload = payload
        
class MqttUnsubAckMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader):
        MqttMsg.__init__(self, fixedHeader, variableHeader)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')

class MqttMsgFactory:
  
    def message(self, fixedHeader, variableHeader=None, payload=None):
        if fixedHeader.messageType == MqttClient.PINGREQ: 
            return MqttPingReqMsg(fixedHeader)
        elif fixedHeader.messageType == MqttClient.PINGRESP: 
            return MqttPingRespMsg(fixedHeader)
        elif fixedHeader.messageType == MqttClient.DISCONNECT: 
            return MqttDisconnetMsg(fixedHeader)
        elif fixedHeader.messageType == MqttClient.CONNECT:
            return MqttConnectMsg(fixedHeader, variableHeader, payload)
        elif fixedHeader.messageType == MqttClient.CONNACK: 
            return MqttConnAckMsg(fixedHeader, variableHeader)
        elif fixedHeader.messageType == MqttClient.PUBLISH: 
            return MqttPublishMsg(fixedHeader, variableHeader, payload)
        elif fixedHeader.messageType == MqttClient.SUBACK: 
            return MqttPubAckMsg(fixedHeader, variableHeader)
        elif fixedHeader.messageType == MqttClient.PUBREC: 
            return MqttPubRecMsg(fixedHeader, variableHeader)
        elif fixedHeader.messageType == MqttClient.PUBREL: 
            return MqttPubRelMsg(fixedHeader, variableHeader)
        elif fixedHeader.messageType == MqttClient.PUBCOMP: 
            return MqttPubCompMsg(fixedHeader, variableHeader)
        elif fixedHeader.messageType == MqttClient.SUBSCRIBE: 
            return MqttSubscribeMsg(fixedHeader, variableHeader, payload)
        elif fixedHeader.messageType == MqttClient.UNSUBSCRIBE: 
            return MqttUnsubscribeMsg(fixedHeader, variableHeader, payload)
        elif fixedHeader.messageType == MqttClient.SUBACK: 
            return MqttSubAckMsg(fixedHeader, variableHeader, payload)
        elif fixedHeader.messageType == MqttClient.UNSUBACK: 
            return MqttUnsubAckMsg(fixedHeader, variableHeader)
        else:
            return None

class MqttClientError(Exception):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class MqttFrameError(Exception):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class MqttDecoderError(Exception):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)

class MqttEncoderError(Exception):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)

    
####HttpClient ###############################################################################    
    
class HTTPResponse:
    __blanklines = ('\r\n', '\n', '') 
    __crlf = '\r\n'
    __continuationChar = '\t'
    __whitespace = " " 
    __readingStatusline = 0
    __readingHeaders = 1
    __readingBody = 2
    __ok = 3
    __continue = 100
    
    def __init__(self, sock, f=None):
        self.__sock = sock
        self.f = f

        
    def response(self):  
        try:
            self.__init()
#             data_block = self.__sock.recv()
            data_block = self.__sock.recv(1500)
            print "AAAAAAAaa"
            print data_block
            print "AAAAAAAaa"
            while(data_block):
                self.__lines = self.__lines + data_block.split(self.__crlf)
                if(len(self.__lines) > 0):
                    if(self.state == self.__readingStatusline):
                        self.__readStatus()
                        # read till we get a non conitnue (100) response
                        if(self.__state == self.__continue): 
                            self.__state = self.__readingStatusline
                            break
#                             data_block = self.__sock.recv(1500)
                    if(self.__state == self.__readingHeaders):
                        self.__readHeaders()
                    if(self.__readingBody):
                        self.__readBody() 
                        break
                if(self.__sock is not None):
#                     data_block = self.__sock.recv()
                    data_block = self.__sock.recv(1500)
                if not data_block:
                    break
        except Exception, e:
            raise HTTPResponseError(str(e))
        return self

    def end(self):
        try:
            if(self.__sock):
                self.__sock.close()
                self.__sock = None
        except:
            pass
        
    def __init(self):
        self.protocol = None
        self.version = None
        self.status = None
        self.reason = None
        self.length = None
        self.close = None 
        self.headers = {}
        self.body = ""  
        self.state = self.__readingStatusline
        self.__lines = []
        self.__lastHeader = None
        
    def __readStatus(self):
        try:
            statusLine = self.__lines.pop(0)
            print "BBBBBBBB"
            print str(self.__lines)
            print "BBBBBBBBBB"
            [version, status, reason] = statusLine.split(None, 2)
        except ValueError:
            try:
                [version, status] = statusLine.split(None, 1)
                reason = ""
            except ValueError:
                version = ""
        if not version.startswith('HTTP'):
            raise HTTPResponseError("Invalid HTTP version in response")
        try:
            status = int(status)
            if status < 100 or status > 999:
                raise HTTPResponseError("HTTP status code out of range 100-999.")
        except ValueError:
            raise HTTPResponseError("Invalid HTTP status code.")
        self.status = status
        self.reason = reason.strip()
        try:
            [protocol, ver] = version.split("/", 2)
        except ValueError:
            raise HTTPResponseError("Invalid HTTP version.")
        self.protocol = protocol
        self.version = ver
        if(self.status == self.__continue): 
            self.__state = self.__continue
        else:
            self.__state = self.__readingHeaders
            
        
    def __readHeaders(self):
        n = len(self.__lines)
        i = 0
        while i < n:
            line = self.__lines.pop(0)
            if(self.__islastLine(line)):
                self.state = self.__readingBody
                break
            if(self.__isContinuationLine(line)):
                [a, b] = line.split(self.__continuationChar, 2)
                self.headers[self.__lastHeader].append(b.strip()) 
            else:
                headerTuple = self.__getHeader(line)
                if(headerTuple):
                    self.headers[headerTuple[0]] = headerTuple[1]
                    self.__lastHeader = headerTuple[0]
            i = i + 1
            
    def __islastLine(self, line):
        return line in self.__blanklines
    
    def __isContinuationLine(self, line):
        if(line.find(self.__continuationChar) > 0): return 1
        else: return 0
    
    def __getHeader(self, line):
        i = line.find(':')
        if i > 0:
            header = line[0:i].lower()
            if(i == len(line)):
                headerValue = []
            else:
                headerValue = line[(i + 1):len(line)].strip()
            if(header == 'content-length' and headerValue):
                try:
                    self.length = int(headerValue)
                except ValueError:
                    self.length = None
                else:
                    if self.length < 0:
                        self.length = None   
            return (header, headerValue)
        return None
    
    def __readBody(self):
        try:
            try:
                if(self.length and self.length != 0 and (self.__lines or self.__sock)):
        #             datablock = self.__sock.recv()
                    if(self.__lines):
                        datablock = self.__lines
                        self.__lines = None
                    else:
                        datablock = self.__sock.recv(1500)
                    length = 0
                    while(datablock and length < self.length):
                        if(isinstance(datablock, list)):
                            datablock = ''.join(datablock)
                        length = length + len(datablock)
                        # Only download body to file if status 200
                        if (self.status == 200 and self.f and hasattr(self.f, 'write')):  
                            self.f.write(datablock)
                        else:
                            self.body = self.body + datablock
    #                     datablock = self.__sock.recv()
                        if(length < self.length):
                            datablock = self.__sock.recv(1500)
                        else: break
                    self.end()
                else:
                    self.end()
            except Exception, e:
                raise Exception(str(e))
        finally:
            self.end()
    
class HTTPClient:
        
    def __init__(self, host, port, userAgent='InstaMsg',enableSsl=0):
        self.version = '1.1'
        self.__userAgent = userAgent
        self.__addr = (host, port)
        self.__sock = None
        self.__checkAddress()
        self.__boundary = '-----------ThIs_Is_tHe_bouNdaRY_78564$!@'
        self.__tcpBufferSize = 1500
        self.__enableSsl =enableSsl
        
    def get(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('GET', url, params, headers, body, timeout)
    
    def put(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('PUT', url, params, headers, body, timeout)
    
    def post(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('POST', url, params, headers, body, timeout)  
    
    def delete(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('DELETE', url, params, headers, body, timeout) 
        
    def uploadFile(self, url, filename, params={}, headers={}, timeout=10):
        filename =str(filename)
        if(not isinstance(filename, str)): raise ValueError('HTTPClient:: upload filename should be of type str.')
        f = None
        try:
            try:
                headers['Content-Type'] = 'multipart/form-data; boundary=%s' % self.__boundary
                form = self.__encode_multipart_fileupload("file", filename)
                fileSize = self.__getFileSize(filename)
                headers['Content-Length'] = len(''.join(form)) + fileSize
                f = open(filename, 'rb')
                return self.__request('POST', url, params, headers, f, timeout, form)  
            except Exception, e:
                if(e.__class__.__name__ == 'HTTPResponseError'):
                    raise HTTPResponseError(str(e))
                raise HTTPClientError("HTTPClient:: %s" % str(e))
        finally:
            self.__closeFile(f)  
    
    def downloadFile(self, url, filename, params={}, headers={}, timeout=10):  
        filename =str(filename)
        url = str(url)
        if(not isinstance(filename, str)): raise ValueError('HTTPClient:: download filename should be of type str.')
        f = None
        response = None
        try:
            try:
                tempFileName = '~' + filename
                f = open(tempFileName, 'wb')
                response = self.__request('GET', url, params, headers, timeout=timeout, fileObject=f)
                f.close()
                if(response.status == 200):
    #               rename(tempFileName, filename)
                    os.rename(tempFileName, filename)
                else:
    #                 unlink(tempFileName)
                    os.remove(tempFileName)
            except Exception, e:
                if(e.__class__.__name__ == 'HTTPResponseError'):
                    raise HTTPResponseError(str(e))
                raise HTTPClientError("HTTPClient:: %s" % str(e))
        finally:
            if f:
                self.__closeFile(f)
        return response
            
    def __closeFile(self, f):   
        try:
            if(f and hasattr(f, 'close')):
                f.close() 
                f = None      
        except:
            pass 
          
    def __getFileSize(self, filename):
        fileSize = None
        try:
            try:
                f = open(filename, 'ab')
                f.seek(0, 2)
                fileSize = f.tell()
            except Exception, e:
                raise Exception(str(e))
        finally:
            if(f and hasattr(f, 'close')):
                f.close()
                f = None
        if(fileSize): return fileSize
        else: raise Exception("HTTPClient:: Unable to determine file size.")
            
    
    def __request(self, method, url, params, headers, body=None, timeout=10, fileUploadForm=None, fileObject=None):
        if(not isinstance(url, str)): raise ValueError('HTTPClient:: url should be of type str.')
        if(not isinstance(params, dict)): raise ValueError('HTTPClient:: params should be of type dictionary.')
        if(not isinstance(headers, dict)): raise ValueError('HTTPClient:: headers should be of type dictionary.')
        if(not isinstance(timeout, int)): raise ValueError('HTTPClient:: timeout should be of type int.')
        if(not(isinstance(body, str) or isinstance(body, file) or body is None)):raise ValueError('HTTPClient:: body should be of type string or file object.')
        try:
            try:
                request = self.__createHttpRequest(method, url, params, headers)
                sizeHint = None
                if(headers.has_key('Content-Length') and isinstance(body, file)):
                    sizeHint = len(request) + headers.get('Content-Length')
                self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if (self.__enableSsl):
                    self.__sock = ssl.wrap_socket(self.__sock, cert_reqs=ssl.CERT_NONE)
                self.__sock.settimeout(timeout)
                self.__sock.connect(self.__addr)
                expect = None
                if(headers.has_key('Expect')):
                    expect = headers['Expect']
                elif(headers.has_key('expect')):
                    expect = headers['expect']
                if(expect and (expect.lower() == '100-continue')):
                    self.__sock.sendall(request)
                    httpResponse = HTTPResponse(self.__sock, fileObject).response()
                    # Send the remaining body if status 100 received or server that send nothing
                    if(httpResponse.status == 100 or httpResponse.status is None):
                        request = ""
                        self.__send(request, body, fileUploadForm, fileObject, sizeHint)
                        return httpResponse.response()
                    else:
                        raise HTTPResponseError("Expecting status 100, recieved %s" % request.status)
                else:
                    self.__send(request, body, fileUploadForm, fileObject, sizeHint)
                    return HTTPResponse(self.__sock, fileObject).response()
            except Exception, e:
                if(e.__class__.__name__ == 'HTTPResponseError'):
                    raise HTTPResponseError(str(e))
                raise HTTPClientError("HTTPClient:: %s" % str(e))
        finally:
            try:
                if(self.__sock):
                    self.__sock.close()
                    self.__sock = None
            except:
                pass
    
    def __send(self, request, body=None, fileUploadForm=None, fileObject=None, sizeHint=None):
        if (isinstance(body, str) or body is None): 
            request = request + (body or "")
            if(request):
                self.__sock.sendall(request)
        else:
            if(fileUploadForm and len(fileUploadForm) == 2):
                blocksize = 1500    
                if(sizeHint <= self.__tcpBufferSize):
                    if hasattr(body, 'read'): 
                        request = request + ''.join(fileUploadForm[0]) + ''.join(body.read(blocksize)) + ''.join(fileUploadForm[1])
                        self.__sock.sendall(request)
                else:
                    request = request + ''.join(fileUploadForm[0])
                    self.__sock.sendall(request)
                    partNumber = 1
                    if hasattr(body, 'read'): 
                        partData = body.read(blocksize)
                        while partData:
        #                             self.__sock.sendMultiPart(partData, partNumber)
                            self.__sock.sendall(partData)
                            partData = body.read(blocksize)
                    if(fileUploadForm and len(fileUploadForm) == 2):
        #                         self.__sock.sendMultiPart(fileUploadForm[1], partNumber + 1)
                        self.__sock.sendall(fileUploadForm[1])
        #                 self.__sock.sendHTTP(self.__addr, request)
    
    def __createHttpRequest(self, method, url, params={}, headers={}):
        url = url + self.__createQueryString(params)
        headers = self.__createHeaderString(headers)
        request = "%s %s %s" % (method, url, headers)
        return request
        
    def __createQueryString(self, params={}):
        i = 0
        query = ''
        for key, value in params.items():
            if(i == 0): 
                query = query + '?%s=%s' % (str(key), str(value))
                i = 1
            else:
                query = query + "&%s=%s" % (str(key), str(value))
        return query
    
    def __createHeaderString(self, headers={}): 
            if(self.__enableSsl):
                httpScheme= "HTTPS"
            else:
                httpScheme= "HTTP"
            headerStr = "%s/%s\r\nHost: %s\r\n" % (httpScheme, self.version, self.__addr[0])
            headers['Connection'] = 'close'  # Only close is supported
            headers['User-Agent'] = self.__userAgent
            for header, values in headers.items():
                if(isinstance(values, list)):
                    headerStr = headerStr + "%s: %s\r\n" % (header, '\r\n\t'.join([str(v) for v in values]))
                else:
                    headerStr = headerStr + "%s: %s\r\n" % (str(header), str(values))
            return headerStr + "\r\n"
        
    def __encode_multipart_fileupload(self, fieldname, filename, contentType='application/octet-stream'):
        formPrefix = []
        crlf = '\r\n'
        formPrefix.append("--" + self.__boundary)
        formPrefix.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (fieldname, filename))
        formPrefix.append('Content-Type: %s' % contentType)
        formPrefix.append('')
        formPrefix.append('')
        return (crlf.join(formPrefix), (crlf + '--' + self.__boundary + '--' + crlf))
            
    def __checkAddress(self):
        if (not self.__addr[0] and not self.__addr[1] and not isinstance(self._addr[1], int)):
            raise ValueError("HTTPClient:: Not a valid HTTP host or port value: %s, %d" % (self.__addr[0], self.__addr[1]))
        

class HTTPResponseError(Exception):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class HTTPClientError(Exception):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class MediaStream:
    def __init__(self, instamsg, uri,clientId,streamId=None, options={}):
        try:
            import gst
            import gtk
        except ImportError:
            raise Exception("Unable to import required libraries for streaming")
        self.gst = gst
        self.gtk = gtk
        self.instamsg = instamsg
        self.uri = uri
        self.streamId= streamId
        self.ipAddress = ''
        self.port=''
        self.clientId = clientId
        self.__qos=1
        self.__dup=0
        self.__mediaTopic = "instamsg/clients/" + self.clientId + "/media"
        self.__mediaReplyTopic = "instamsg/clients/" + self.clientId + "/mediareply"
        self.__mediaStopTopic = "instamsg/clients/" + self.clientId + "/mediastop"
        self.__mediaPauseTopic = "instamsg/clients/" + self.clientId + "/mediapause"
        self.__initStreaming()
        
    def __initStreaming(self):
        self.__subscribe(self.__mediaReplyTopic, self.__qos)
        time.sleep(30)
        self.__publishMediaMessage(self.__mediaTopic)

    def broadcast(self,sdpAnswer):
        self.__processOffer(sdpAnswer)
# 
    def stop(self):
        self.pipeline.set_state(self.pipeline.set_state(self.gst.STATE_NULL))
#    
    def paused(self):
        self.pipeline.set_state(self.gst.STATE_PAUSED)
        
    def __publishMediaMessage(self,topic, qos=1, dup=0):
#         ip_address = __get_ip_address("wlan0")
        ip_address = "192.168.1.13"
        
        sdpOffer  = "v=0\r\n";
        sdpOffer += "o=- 0 0 IN IP4 " + ip_address + "\r\n";
        sdpOffer += "s=\r\n";
        sdpOffer += "c=IN IP4 " + ip_address + "\r\n";
        sdpOffer += "t=0 0\r\n";
        sdpOffer += "a=charset:UTF-8\n";
        sdpOffer += "a=recvonly\r\n";
        sdpOffer += "m=video 50004 RTP/AVP 96\r\n";
        sdpOffer += "a=rtpmap:96 H264/90000\r\n";
        
        message = {
                    'to': self.clientId,
                    'sdp_offer' : sdpOffer,
                    'from ': self.clientId,
                    'protocol' : 'rtp',
                    'type':'7',
                    'record': True
                }
        self.__publish(topic, str(message), qos, dup)
        
    def __publish(self, topic, msg, qos, dup):
        try:
            def _resultHandler(result):
                print "Published message %s to topic %s with qos %d" %(msg, topic,qos)
            self.instamsg.publish(topic, msg, qos, dup, _resultHandler)
        except Exception, e:
            self.instamsg.log(self.instamsg.INSTAMSG_LOG_LEVEL_DEBUG,e)
        
    def __subscribe(self, topic, qos=1):
        try:
            def _resultHandler(result):
                print "Subscribed to topic %s with qos %d" %(topic,qos)
            self.instamsg.subscribe(topic, qos, self.__messageHandler, _resultHandler)
        except Exception, e:
            self.instamsg.log(self.instamsg.INSTAMSG_LOG_LEVEL_DEBUG,e)
            
    def __messageHandler(self,mqttMessage):
        if(mqttMessage):
            self.instamsg.log(self.instamsg.INSTAMSG_LOG_LEVEL_DEBUG,"Media streamer received message %s" %str(mqttMessage.toString()))
            if(mqttMessage.topic() == self.__mediaReplyTopic):
                self.__handleMediaReplyMessage(mqttMessage)
            if(mqttMessage.topic() == self.__mediaPauseTopic):
                self.paused()
            if(mqttMessage.topic() == self.__mediaStopTopic):
                self.stop()
                
    def __handleMediaReplyMessage(self,mqttMessage):
        msgJson = self.__parseJson(mqttMessage.body())
        if(msgJson is not None and( msgJson.has_key('sdp_answer'))):
            self.broadcast(msgJson['sdp_answer'])
            
    def __parseJson(self,jsonString):
        return json.loads(jsonString)
    
    def __processOffer(self, sdpAnswer):
        import re
        
        isAddress = re.search('o=- 0 (.+?) IN IP4 (.+?)\r\ns=', sdpAnswer)
        if isAddress:
            self.ipAddress = isAddress.group(2)
            
        isPort = re.search('m=video (.+?) RTP/AVP 96',sdpAnswer)
        if isPort:
            self.port = isPort.group(1)

        self.__createStreamingPipline()
     
    def __createStreamingPipline(self):
        pipe = (self.uri + " !  udpsink host=%s port=%s" %(self.ipAddress, self.port))
        
        print pipe
        
        self.pipeline = self.gst.parse_launch(pipe)

        self.pipeline.set_state(self.gst.STATE_PLAYING)
        
        self.instamsg.log(self.instamsg.INSTAMSG_LOG_LEVEL_DEBUG,"Media streamering started on %s" %str(self.port))
        self.gtk.main()
              
