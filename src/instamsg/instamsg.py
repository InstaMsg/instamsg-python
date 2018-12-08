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
import traceback

from ..mqtt.client import MqttClient
from ..mqtt.result import Result
from ..mqtt.constants import PROVISIONING_CLIENT_ID
from .message import Message
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
        self.__onConnectCallBack = connectHandler   
        self.__onDisConnectCallBack = disConnectHandler  
        self.__oneToOneMessageHandler = oneToOneMessageHandler
        self.__authHash = None
        self.__init(clientId, authKey)
        self.__logsListener = []
        self.__defaultReplyTimeout = INSTAMSG_RESULT_HANDLER_TIMEOUT
        self.__msgHandlers = {}
        self.__sendMsgReplyHandlers = {}  # {handlerId:{time:122334,handler:replyHandler, timeout:10, timeOutMsg:"Timed out"}}
        self.__enableTcp = 1
        self.__enableSsl = 1
        self.__configHandler = None
        self.__rebootHandler = None
        self.__metadata = {}
        self.__mqttClient = None
        self.__initOptions(options)
        if(self.__enableTcp):
            clientIdAndUsername = self.__getClientIdAndUsername(clientId)
            mqttoptions = self.__mqttClientOptions(clientIdAndUsername[1], authKey, self.__keepAliveTimer)
            self.__mqttClient = MqttClient(INSTAMSG_HOST, self.__port, clientIdAndUsername[0], enableSsl=self.__enableSsl, options=mqttoptions)
            self.__mqttClient.onConnect(self.__onConnect)
            self.__mqttClient.onDisconnect(self.__onDisConnect)
            self.__mqttClient.onDebugMessage(self.__handleDebugMessage)
            self.__mqttClient.onMessage(self.__handleMessage)
            self.__connected = 0    
            self.__mqttClient.connect()
        else:
            #Try websocket
            self.__mqttClient = None
 
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
            if(not callable(options['configHandler'])): raise ValueError('configHandler should be a callable object.')
            self.__configHandler = options['configHandler']
        if( 'rebootHandler' in options): 
            if(not callable(options['rebootHandler'])): raise ValueError('rebootHandler should be a callable object.')
            self.__rebootHandler = options['rebootHandler']
        if('enableTcp' in options):
            self.__enableTcp = options.get('enableTcp')
        if('enableLogToServer' in options):
            self.__enableLogToServer = options.get('enableLogToServer')
        else: self.__enableLogToServer = 0
        if('logLevel' in options):
            self.__logLevel = options.get('logLevel')
            if(self.__logLevel < INSTAMSG_LOG_LEVEL_DISABLED or self.__logLevel > INSTAMSG_LOG_LEVEL_DEBUG):
                raise ValueError("logLevel option should be in between %d and %d" % (INSTAMSG_LOG_LEVEL_DISABLED, INSTAMSG_LOG_LEVEL_DEBUG))
        else: self.__logLevel = INSTAMSG_LOG_LEVEL_DISABLED
        if('keepAliveTimer' in options):
            self.__keepAliveTimer = options.get('keepAliveTimer')
        else:
            self.__keepAliveTimer = INSTAMSG_KEEP_ALIVE_TIMER
        if('metadata' in options):
            self.__metadata = options['metadata']
        self.__metadata["instamsg_version"] = INSTAMSG_VERSION
        self.__metadata["instamsg_api_version"] = INSTAMSG_API_VERSION     
        if('enableSsl' in options and options.get('enableSsl')): 
            if(HAS_SSL):
                self.__enableSsl = 1
                self.__port = INSTAMSG_PORT_SSL 
            else:
                raise ImportError("SSL not supported, Please check python version and try again.")
        else: 
            self.__enableSsl = 0 
            self.__port = INSTAMSG_PORT

    
    def run(self):
        try:
            while self.alive.isSet():
                try:
                    if(self.__mqttClient is not None):
                        self.__mqttClient.process()
                        self.__processHandlersTimeout()
                except Exception as e:
                    self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsgClientError, method = run]- %s" % (str(e)))
                    self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_DEBUG, "[InstaMsgClientError]- %s" % (traceback.print_exc()))                 
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
                self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_DEBUG, "[InstaMsgClientError]- %s" % (traceback.print_exc()))
                raise InstaMsgPubError(str(e))
        else: raise ValueError("Topic cannot be null or empty string.")
    
    def subscribe(self, topic, qos, msgHandler, resultHandler=None, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
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
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_DEBUG, "[InstaMsgClientError]- %s" % (traceback.print_exc()))
            raise InstaMsgSubError(str(e))
            

    def unsubscribe(self, topics, resultHandler, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        try:
            def _resultHandler(result):
                if(result.succeeded()):
                    for topic in topics:
                        if(topic in self.__msgHandlers):
                            del self.__msgHandlers[topic]
                if(callable(resultHandler)):resultHandler(result)
            self.__mqttClient.unsubscribe(topics, _resultHandler, timeout)
        except Exception as e:
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_DEBUG, "[InstaMsgClientError]- %s" % (traceback.print_exc()))
            raise InstaMsgUnSubError(str(e))

    
    def send(self, clienId, msg, qos=INSTAMSG_QOS0, dup=0, replyHandler=None, timeout=INSTAMSG_MSG_REPLY_HANDLER_TIMEOUT):
        try:
            messageId = self._generateMessageId()
            msg = Message(messageId, clienId, msg, qos, dup, replyTopic=self.__clientId, instaMsg=self)._sendMsgJsonString()
            self._send(messageId, clienId, msg, qos, dup, replyHandler, timeout)
        except Exception as e:
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_DEBUG, "[InstaMsgClientError]- %s" % (traceback.print_exc()))
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
                if(callable(provisionHandler)): provisionHandler (provResponse)
                _log(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]::Provisioning completed.")
            else:
                _log(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]::Provisioning failed.")
        except Exception as e:
            _log(INSTAMSG_LOG_LEVEL_DEBUG, "[InstaMsgClientError]- %s" % (traceback.print_exc()))  
            raise InstaMsgProvisionError(str(e))


    def publishConfig(self, config, resultHandler=None):
        try:
            config["instamsg_version"] = INSTAMSG_API_VERSION
            message = json.dumps(config)
            def _resultHandler(result):
                if(result.failed()):
                    if(callable(resultHandler)):resultHandler(Result(config,0,result.cause()))  
                    self.log(INSTAMSG_LOG_LEVEL_ERROR, "[InstaMsg]::Error publishing Config to server: %s" %str(result.cause()))  
                else:
                    if(callable(resultHandler)):resultHandler(Result(config,1))
                    self.log(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]::Config published to server: %s" %str(config))  
            self.publish(self.__configClientToServerTopic, message, qos=INSTAMSG_QOS1, dup=0, resultHandler=_resultHandler)
        except Exception as e:
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_DEBUG, "[InstaMsgClientError]- %s" % (traceback.print_exc()))   

    def publishNetworkInfo(self, networkInfo):
        networkInfo["instamsg_version"] = INSTAMSG_VERSION
        networkInfo["instamsg_api_version"] = INSTAMSG_API_VERSION
        message = json.dumps(networkInfo)
        self.publish(self.__networkInfoTopic, message, INSTAMSG_QOS0, 0)

    
    def _send(self, messageId, clienId, msg, qos, dup, replyHandler, timeout):
        try:
            if(replyHandler):
                timeOutMsg = "Sending message[%s] %s to %s timed out." % (str(messageId), str(msg), str(clienId))
                self.__sendMsgReplyHandlers[messageId] = {'time':time.time(), 'timeout': timeout, 'handler':replyHandler, 'timeOutMsg':timeOutMsg}
                def _resultHandler(result):
                    if(result.failed()):
                        if(messageId in self.__sendMsgReplyHandlers):
                            del self.__sendMsgReplyHandlers[messageId]
                    if(callable(replyHandler)):replyHandler(result)
            else:
                _resultHandler = None
            self.publish(clienId, msg, qos, dup, _resultHandler)
        except Exception as e:
            if(messageId in self.__sendMsgReplyHandlers):
                del self.__sendMsgReplyHandlers[messageId]
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_DEBUG, "[InstaMsgClientError]- %s" % (traceback.print_exc()))
            raise Exception(str(e))


    def _generateMessageId(self):
        messageId = self.__clientId + "-" + str(int(time.time() * 1000))
        while(messageId in self.__sendMsgReplyHandlers):
            messageId = self.__clientId + "-" + str(int(time.time() * 1000))
        return messageId;
    
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
        self.__sendClientMetadata()
        self.subscribe(self.__enableServerLoggingTopic, INSTAMSG_QOS0, self.__enableServerLogging)
        time.sleep(10)
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
            elif(mqttMsg.topic == self.__rebootTopic):
                self.__handleSystemRebootMessage()
            elif(mqttMsg.topic == self.__configServerToClientTopic):
                self.__handleConfigMessage(mqttMsg)
            elif(mqttMsg.topic == self.__enableServerLoggingTopic):
                self.__enableServerLogging(mqttMsg)
            else:
                self.__handlePubSubMessage(mqttMsg)

        except Exception as e:
            self.__handleDebugMessage(INSTAMSG_LOG_LEVEL_DEBUG, "[InstaMsgClientError]- %s" % (traceback.print_exc()))                
                 
            
    def __handlePubSubMessage(self, mqttMsg):     
        msgHandler = self.__msgHandlers.get(mqttMsg.topic)
        if(msgHandler and callable(msgHandler)):
            msg = Message(mqttMsg.messageId, mqttMsg.topic, mqttMsg.payload, mqttMsg.fixedHeader.qos, mqttMsg.fixedHeader.dup)
            msgHandler(msg)
        
    def __handlePointToPointMessage(self, mqttMsg):
        try:
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
                    if(callable(msgHandler)): msgHandler(result)
                    del self.__sendMsgReplyHandlers[responseId]
            else:
                if(self.__oneToOneMessageHandler):
                    msg = Message(messageId, self.__clientId, body, qos, dup, replyTopic=replyTopic, instaMsg=self)
                    self.__oneToOneMessageHandler(msg)
        except json.JSONDecodeError as e:
            #This could be a normal message published using publish 
            #e.g. loopback publish. Handle as normal pub message
            self.__handlePubSubMessage(mqttMsg)

        
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
        return json.loads(jsonString)
    
    def __processHandlersTimeout(self): 
        for key in list(self.__sendMsgReplyHandlers):
            value = self.__sendMsgReplyHandlers[key]
            if((time.time() - value['time']) >= value['timeout']):
                resultHandler = value['handler']
                if(resultHandler):
                    timeOutMsg = value['timeOutMsg']
                    if(callable(resultHandler)): resultHandler(Result(None, 0, (INSTAMSG_ERROR_TIMEOUT, timeOutMsg)))
                    value['handler'] = None
                del self.__sendMsgReplyHandlers[key]
  

    def __sendClientMetadata(self):
        try:
            message = json.dumps(self.__metadata)
            self.publish(self.__infoTopic, message, INSTAMSG_QOS0, 0)
        except:
            self.log(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]::Error publishing client metadata. Continuing...")
    
    def __handleConfigMessage(self, mqttMsg):
        msgJson = self.__parseJson(mqttMsg.payload)
        if(callable(self.__configHandler)):
            self.__configHandler(Result((msgJson),1))

    def __handleSystemRebootMessage(self):
        self.log(INSTAMSG_LOG_LEVEL_INFO, "[InstaMsg]::Rebooting device.")
        if(callable(self.__rebootHandler)):
            self.__rebootHandler() 