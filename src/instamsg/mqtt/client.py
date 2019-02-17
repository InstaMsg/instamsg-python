# -*- coding: utf-8 -*-
import sys
import os
import time
import json
import fcntl
import struct
import traceback
import _thread
from threading import Thread, Event, RLock 
import socket
try:
    import wolfssl
except:
    pass

from .messages import MqttFixedHeader, MqttMsgFactory
from .decoder import MqttDecoder
from .encoder import MqttEncoder
from .result import Result
from .errors import *
from .constants import *


class MqttClient:

    def __init__(self, host, port, clientId, enableSsl=0, options={}):
        if(not clientId):
            raise ValueError('MqttClient:: clientId cannot be null.')
        if(len(clientId) > MQTT_MAX_CLIENT_ID_LENGTH):
            raise ValueError('MqttClient:: clientId cannot length cannot be greater than %s.' % MQTT_MAX_CLIENT_ID_LENGTH)
        if(not host):
            raise ValueError('MqttClient:: host cannot be null.')
        if(not port):
            raise ValueError('MqttClient:: port cannot be null.')
        self.errCount = 0
        self.lock = RLock()
        self.host = host
        self.port = port
        self.clientId = clientId
        self.enableSsl= enableSsl
        self.options = self._initOptions(options)
        self.keepAliveTimer = self.options['keepAliveTimer']
        self.reconnectTimer = self.options['reconnectTimer']
        self._logLevel = options.get('logLevel')
        self._cleanSession = 1
        self._sock = None
        self._sockInit = 0
        self._connected = 0
        self._connecting = 0
        self._disconnecting = 0
        self._waitingReconnect = 0
        self._nextConnTry = time.time()
        self._lastPingReqTime = time.time()
        self._lastPingRespTime = self._lastPingReqTime
        self._mqttMsgFactory = MqttMsgFactory()
        self._mqttEncoder = MqttEncoder()
        self._mqttDecoder = MqttDecoder()
        self._messageId = 0
        self._onDisconnectCallBack = None
        self._onConnectCallBack = None
        self._onMessageCallBack = None
        self._onDebugMessageCallBack = None
        self._msgIdInbox = []
        self._resultHandlers = {}  # {handlerId:{time:122334,handler:replyHandler, timeout:10, timeOutMsg:"Timed out"}}
        self._lastConnectTime = time.time()
        self.decoderErrorCount = 0
        self._connectAckTimeoutCount = 0

    def _initOptions(self, options):
        options['clientId'] = self.clientId
        if(not 'hasUserName' in options): options['hasUserName'] = 0 
        if(not 'username' in options): options['username'] = ''
        if(not 'hasPassword' in options): options['hasPassword'] = 0 
        if(not 'password' in options): options['password'] = ''
        if(not 'keepAliveTimer' in options): options['keepAliveTimer'] = 60 
        if(options['keepAliveTimer'] > MQTT_MAX_KEEP_ALIVE_TIMER ): raise ValueError("keepAliveTimer should be less than 64800")
        if(not 'hasPassword' in options): options['hasPassword'] = 0 
        if(not 'isCleanSession' in options): options['isCleanSession'] = 1     
        if(not 'isWillFlag' in options): options['isWillFlag'] = 0
        if(not 'willQos' in options): options['willQos'] = 0
        if(not 'isWillRetain' in options): options['isWillRetain'] = 0
        if(not 'willTopic' in options): options['willTopic'] = ""
        if(not 'willMessage' in options): options['willMessage'] = ""
        if(not 'logLevel' in options): options['logLevel'] = MQTT_LOG_LEVEL_DEBUG
        if(not 'reconnectTimer' in options): options['reconnectTimer'] = MQTT_SOCKET_RECONNECT_TIMER   
        return options  
        
    def process(self):
        try:
            if(not self._disconnecting):
                self.connect()
                if(self._sockInit):
                    mqttMsg = self._receive()
                    if (mqttMsg): self._handleMqttMessage(mqttMsg)
                    if ((self._lastPingReqTime + (1 + MQTT_KEEP_ALIVE_TIMER_GRACE) * self.keepAliveTimer) < time.time()):
                        if (self._lastPingRespTime is None):
                            self.disconnect()
                        else: 
                            self._sendPingReq()
                            self._lastPingReqTime = time.time()
                            self._lastPingRespTime = None
                self._processHandlersTimeout()
                if (self._connecting and ((self._lastConnectTime + CONNECT_ACK_TIMEOUT) < time.time())):
                    self._log(MQTT_LOG_LEVEL_INFO, "[MqttClientError, method = process]::Connect Ack timed out. Reseting connection.")
                    self._resetSock()
        except socket.error as msg:
            self._resetSock()
            self._log(MQTT_LOG_LEVEL_ERROR, "[MqttClientError, method = process][SocketError]:: %s" % (str(msg)))
            self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = process][MqttConnectError]:: %s" % (traceback.print_exc()))
        except MqttConnectError as msg:
            self._log(MQTT_LOG_LEVEL_ERROR, "[MqttClientError, method = process][MqttConnectError]:: %s" % (str(msg)))
            self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = process][MqttConnectError]:: %s" % (traceback.print_exc()))
        except:
            self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = process][MqttConnectError]:: %s" % (traceback.print_exc()))
            

    def provision(self, provId, provPin, timeout = 300):
        try:
            auth = None
            self._initSock()
            if(self._sockInit):
                options = self.options.copy()
                options['clientId'] = PROVISIONING_CLIENT_ID
                options['hasUserName'] = 1
                if (provPin):
                    options['hasPassword'] = 1
                else:
                    options['hasPassword'] = 0
                options['username'] = provId
                options['password'] = provPin
                fixedHeader = MqttFixedHeader(CONNECT, qos=0, dup=0, retain=0)
                provisionMsg = self._mqttMsgFactory.message(fixedHeader, options, options)
                encodedMsg = self._mqttEncoder.encode(provisionMsg)
                self._sendall(encodedMsg)
                timeout = time.time() + timeout
                while(timeout > time.time()):
                    time.sleep(10)
                    mqttMsg = self._receive()
                    if(mqttMsg and mqttMsg.fixedHeader.messageType == PROVACK):
                        auth = self._getAuthInfoFromProvAckMsg(mqttMsg)
                        break
                return auth
        finally:
            self._closeSocket()

    def connect(self):
        try:
            self._initSock()
            if(self._connecting is 0 and self._sockInit):
                if(not self._connected):
                    self._connecting = 1
                    self._log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Mqtt Connecting to %s:%s' % (self.host, str(self.port)))   
                    fixedHeader = MqttFixedHeader(CONNECT, qos=0, dup=0, retain=0)
                    connectMsg = self._mqttMsgFactory.message(fixedHeader, self.options, self.options)
                    encodedMsg = self._mqttEncoder.encode(connectMsg)                    
                    self._sendall(encodedMsg)
        except socket.timeout:
            self._connecting = 0
            self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = connect][SocketTimeoutError]:: Socket timed out sending connect")
        except socket.error as msg:
            self._resetSock()
            self._log(MQTT_LOG_LEVEL_ERROR, "[MqttClientError, method = connect][SocketError]:: %s" % (str(msg)))
            self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = connect][Exception]:: %s" % (traceback.print_exc()))
        except:
            self._connecting = 0
            self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = connect][Exception]:: %s" % (traceback.print_exc()))
    
    def disconnect(self):
        try:
            try:
                self._disconnecting = 1
                if(not self._connecting  and not self._waitingReconnect and self._sockInit):
                    fixedHeader = MqttFixedHeader(DISCONNECT, qos=0, dup=0, retain=0)
                    disConnectMsg = self._mqttMsgFactory.message(fixedHeader)
                    encodedMsg = self._mqttEncoder.encode(disConnectMsg)
                    self._sendall(encodedMsg)
            except Exception as msg:
                self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = disconnect][Exception]:: %s" % (traceback.print_exc()))
        finally:
            self._resetSock()
    
    def publish(self, topic, payload, qos=MQTT_QOS0, dup=0, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT, retain=0, logging=1):
        if(not self._connected or self._connecting  or self._waitingReconnect):
            raise MqttClientError("Cannot publish message as not connected.")
        self._validateTopic(topic)
        self._validateQos(qos)
        self._validateResultHandler(resultHandler)
        self._validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(PUBLISH, qos, dup=0, retain=0)
        messageId = 0
        if(qos > MQTT_QOS0): messageId = self._generateMessageId()
        variableHeader = {'messageId': messageId, 'topic': str(topic)}
        publishMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader, payload)
        encodedMsg = self._mqttEncoder.encode(publishMsg)
        if(logging):
            self._log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: sending message:%s' % publishMsg.toString())
        if (qos > MQTT_QOS0 and messageId and resultHandler): 
            timeOutMsg = 'Publishing message %s to topic %s with qos %d timed out.' % (payload, topic, qos)
            self._resultHandlers[messageId] = {'time':time.time(), 'timeout': resultHandlerTimeout, 'handler':resultHandler, 'timeOutMsg':timeOutMsg}
        self._sendall(encodedMsg)      
        if(qos == MQTT_QOS0 and resultHandler and callable(resultHandler)): 
            resultHandler(Result(None, 1))  # immediately return messageId 0 in case of qos 0

        
    def subscribe(self, topic, qos, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT):
        if(not self._connected or self._connecting  or self._waitingReconnect):
            raise MqttClientError("Cannot subscribe as not connected.")
        self._validateTopic(topic)
        self._validateQos(qos)
        self._validateResultHandler(resultHandler)
        self._validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(SUBSCRIBE, qos=1, dup=0, retain=0)
        messageId = self._generateMessageId()
        variableHeader = {'messageId': messageId}
        subMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader, {'topic':topic, 'qos':qos})
        encodedMsg = self._mqttEncoder.encode(subMsg)
        self._log(MQTT_LOG_LEVEL_DEBUG, '[MqttClient]:: sending subscribe message: %s' % subMsg.toString())
        if(resultHandler):
            timeOutMsg = 'Subscribe to topic %s with qos %d timed out.' % (topic, qos)
            self._resultHandlers[messageId] = {'time':time.time(), 'timeout': resultHandlerTimeout, 'handler':resultHandler, 'timeOutMsg':timeOutMsg}
        self._sendall(encodedMsg)
                
    def unsubscribe(self, topics, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT):
        if(not self._connected or self._connecting  or self._waitingReconnect):
            raise MqttClientError("Cannot unsubscribe as not connected.")
        self._validateResultHandler(resultHandler)
        self._validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(UNSUBSCRIBE, qos=1, dup=0, retain=0)
        messageId = self._generateMessageId()
        variableHeader = {'messageId': messageId}
        if(isinstance(topics, str)):
            topics = [topics]
        if(isinstance(topics, list)):
            for topic in topics:
                self._validateTopic(topic)
                unsubMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader, topics)
                encodedMsg = self._mqttEncoder.encode(unsubMsg)
                self._log(MQTT_LOG_LEVEL_DEBUG, '[MqttClient]:: sending unsubscribe message: %s' % unsubMsg.toString())
                if(resultHandler):
                    timeOutMsg = 'Unsubscribe to topics %s timed out.' % str(topics)
                    self._resultHandlers[messageId] = {'time':time.time(), 'timeout': resultHandlerTimeout, 'handler':resultHandler, 'timeOutMsg':timeOutMsg}
                self._sendall(encodedMsg)
                return messageId
        else:   raise TypeError('Topics should be an instance of string or list.') 
    
    def onConnect(self, callback):
        if(callable(callback)):
            self._onConnectCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.')
    
    def connected(self):
        return self._connected  and not self._disconnecting
        
    def onDisconnect(self, callback):
        if(callable(callback)):
            self._onDisconnectCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.')
        
    def onDebugMessage(self, callback):
        if(callable(callback)):
            self._onDebugMessageCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.') 
    
    def onMessage(self, callback):
        if(callable(callback)):
            self._onMessageCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.')   

    def _setSocketNConnect(self):
        self._log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Opening socket to %s:%s' % (self.host, str(self.port)))
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)       
        if(self.enableSsl):
            # wolfssl.WolfSSL.enable_debug()
            context = wolfssl.SSLContext(wolfssl.PROTOCOL_TLSv1_2) 
            # context.load_cert_chain(certificate, certificate_key)
            context.verify_mode = wolfssl.CERT_NONE
            # context.set_ciphers("")
            self._sock = context.wrap_socket(self._sock)
        self._sock.connect((self.host, self.port))
        self._log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Socket opened to %s:%s' % (self.host, str(self.port))) 
    

    def _getDataFromSocket(self):
        try:
            if self._sock is not None:
                return self._sock.read(MQTT_SOCKET_MAX_BYTES_READ)
        except (wolfssl.exceptions.SSLWantWriteError, wolfssl.exceptions.SSLWantReadError):
            pass
        except socket.timeout:
            pass
        except socket.error as msg:
            # if 'timed out' in msg.message.lower():
            #     # Hack as openssl library does not throw timeout error
            #     pass
            # else:
            self._resetSock()
            self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = _receive][SocketError]:: %s" % (str(msg)))
        

    def _sendallDataToSocket(self, data):
        try:
            if self._sock is not None:
                self._sock.sendall(data)
        except (wolfssl.exceptions.SSLWantWriteError, wolfssl.exceptions.SSLWantReadError):
            pass
        except socket.error as msg:                  
            self._resetSock()
            raise socket.error(str("Socket error in send: %s. Connection reset." % (str(msg))))
        

    def _validateTopic(self, topic):
        if(topic):
            pass
        else: raise ValueError('Topics cannot be Null or empty.')
        if (len(topic) < MQTT_MAX_TOPIC_LEN + 1):
            pass
        else:
            raise ValueError('Topic length cannot be more than %d' % MQTT_MAX_TOPIC_LEN)
        
    def _validateQos(self, qos):
        if(not isinstance(qos, int) or qos < MQTT_QOS0 or qos > MQTT_QOS2):
            raise ValueError('Qos should be a between %d and %d.' % (MQTT_QOS0, self.MQTT_QOS2)) 
        
    def _validateRetain(self, retain):
        if (not isinstance(retain, int) or retain != 0 or retain != 1):
            raise ValueError('Retain can only be integer 0 or 1')
        
    def _validateTimeout(self, timeout):
        if (not isinstance(timeout, int) or timeout < 0 or timeout > MQTT_MAX_RESULT_HANDLER_TIMEOUT):
            raise ValueError('Timeout can only be integer between 0 and %d.' % MQTT_MAX_RESULT_HANDLER_TIMEOUT)
        
    def _validateResultHandler(self, resultHandler):
        if(resultHandler is not None and not callable(resultHandler)):            
            raise ValueError('Result Handler should be a callable object.') 
            
    def _log(self, level, msg):
        if(level <= self._logLevel):
            if(self._onDebugMessageCallBack):
                self._onDebugMessageCallBack(level, msg)

    def _sendall(self, data):
        self.lock.acquire()
        try:
            if(data):
                self._sendallDataToSocket(data)
        finally:
            self.lock.release()
      
    def _receive(self):
        self.lock.acquire()
        try:
            try:
                data = self._getDataFromSocket()
                if (data is not None) and (len(data) > 0): 
                    mqttMsg = self._mqttDecoder.decode(data)
                    self.decoderErrorCount = 0
                    if (mqttMsg):
                        self._log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Received message:%s' % mqttMsg.toString())
                else:
                    mqttMsg = None
                return mqttMsg 
            except MqttDecoderError as msg:
                self.decoderErrorCount = self.decoderErrorCount + 1
                self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = _receive][MqttDecoderError]:: %s" % (traceback.print_exc()))
                if (self.decoderErrorCount > MQTT_DECODER_ERROR_COUNT_FOR_SOCKET_RESET):
                    self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = _receive]:: Resetting socket as MqttDecoderError count exceeded %s" % (str(MQTT_DECODER_ERROR_COUNT_FOR_SOCKET_RESET)))
                    self.decoderErrorCount = 0
                    self._resetSock()                    
            except MqttFrameError as msg:
                self._resetSock()
                self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = _receive][MqttFrameError]:: %s" % (traceback.print_exc()))                
        finally:
            self.lock.release() 

  
    def _handleMqttMessage(self, mqttMessage):
        self._lastPingRespTime = time.time()
        msgType = mqttMessage.fixedHeader.messageType
        if msgType == CONNACK:
            self._handleConnAckMsg(mqttMessage)
        elif msgType == PUBLISH:
            self._handlePublishMsg(mqttMessage)
        elif msgType == SUBACK:
            self._handleSubAck(mqttMessage)
        elif msgType == UNSUBACK:
            self._handleUnSubAck(mqttMessage)
        elif msgType == PUBACK:
            self._handlePubAckMsg(mqttMessage)
        elif msgType == PUBREC:
            self._handlePubRecMsg(mqttMessage)
        elif msgType == PUBCOMP:
            self._onPublish(mqttMessage)
        elif msgType == PUBREL:
            self._handlePubRelMsg(mqttMessage)
        elif msgType == PINGRESP:
            self._lastPingRespTime = time.time()
        elif msgType in [CONNECT, PROVACK, SUBSCRIBE, UNSUBSCRIBE, PINGREQ]:
            pass  # Client will not receive these messages
        else:
            raise MqttClientError('[MqttClientError, method = _handleMqttMessage]:: Unknown message type.') 
    
    def _getResultHandler(self, mqttMessage):
        resultHandler = None
        resultHandlerDict = self._resultHandlers.get(mqttMessage.messageId)
        if(resultHandlerDict):
            resultHandler = resultHandlerDict.get('handler')
        return resultHandler
    
    def _handleSubAck(self, mqttMessage):
        resultHandler = self._getResultHandler(mqttMessage)
        if(resultHandler):
            if(callable(resultHandler)): resultHandler(Result(mqttMessage, 1))
            del self._resultHandlers[mqttMessage.messageId]
    
    def _handleUnSubAck(self, mqttMessage):
        resultHandler = self._getResultHandler(mqttMessage)
        if(resultHandler):
            if(callable(resultHandler)): resultHandler(Result(mqttMessage, 1))
            del self._resultHandlers[mqttMessage.messageId]
    
    def _onPublish(self, mqttMessage):
        resultHandler = self._getResultHandler(mqttMessage)
        if(resultHandler):
            if(callable(resultHandler)): resultHandler(Result(mqttMessage, 1))
            del self._resultHandlers[mqttMessage.messageId]
    
    def _handleConnAckMsg(self, mqttMessage):
        self._connecting = 0
        connectReturnCode = mqttMessage.connectReturnCode
        if(connectReturnCode == CONNECTION_ACCEPTED):
            self._connected = 1
            self._lastConnectTime = time.time()
            self._log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Connected to %s:%s' % (self.host, str(self.port)))   
            if(self._onConnectCallBack): self._onConnectCallBack(self)  
        else:
            self._handleProvisionAndConnectAckCode("Connection", connectReturnCode)

            
    def _getAuthInfoFromProvAckMsg(self, mqttMessage):
        provisionReturnCode = mqttMessage.provisionReturnCode
        payload = mqttMessage.payload
        provisioningData = {}
        if (not (payload and provisionReturnCode in [PROVISIONING_SUCCESSFUL, PROVISIONING_SUCCESSFUL_WITH_CERT])):
            return provisioningData
        if(provisionReturnCode == PROVISIONING_SUCCESSFUL):
            provisioningData['client_id']=payload[0:36] 
            provisioningData['auth_token'] = payload[37:]
            provisioningData['secure_ssl_certificate'] = 0
            provisioningData['key'] = ''
            provisioningData['certificate'] = ''
            return provisioningData
        elif(provisionReturnCode == PROVISIONING_SUCCESSFUL_WITH_CERT):
            payloadJson = json.loads(jsonString)
            keys = ['client_id', 'auth_token', 'secure_ssl_certificate', 'key', 'certificate']   
            for key in keys:             
                if (key in payloadJson):
                    if (key in ['key', 'certificate']):
                        provisioningData[key] = payloadJson[key].replace("\\\\n", "\n")
                    else:
                        provisioningData[key] = payloadJson[key]
                else:
                    provisioningData[key] = ''
            return provisioningData
        else:
            self._handleProvisionAndConnectAckCode("Provisioning", provisionReturnCode)
            
            
    def _handleProvisionAndConnectAckCode(self, type, code):
        msg =''
        if(code == CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION):
            msg = '[MqttClient]:: %s refused unacceptable mqtt protocol version.' % type
        elif(code == CONNECTION_REFUSED_IDENTIFIER_REJECTED):
            msg =  '[MqttClient]:: %s refused client identifier rejected.' % type
        elif(code == CONNECTION_REFUSED_SERVER_UNAVAILABLE): 
            msg =  '[MqttClient]:: %s refused server unavailable. Waiting ...' % type
            self._log(MQTT_LOG_LEVEL_DEBUG, msg)
            return # Not an error just wait for server
        elif(code == CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD):  
            msg = '[MqttClient]:: %s refused bad username or password.' % type
        elif(code == CONNECTION_REFUSED_NOT_AUTHORIZED):  
            msg = '[MqttClient]:: %s refused not authorized.' % type
        raise MqttConnectError(msg) # Error should be bubbled up
    
    def _handlePublishMsg(self, mqttMessage):
        if(mqttMessage.fixedHeader.qos > MQTT_QOS1): 
            if(mqttMessage.messageId not in self._msgIdInbox):
                self._msgIdInbox.append(mqttMessage.messageId)
        if(self._onMessageCallBack):
            self._onMessageCallBack(mqttMessage)
        if(mqttMessage.fixedHeader.qos == MQTT_QOS1):
            self._sendPubAckMsg(mqttMessage)

    def _sendPubAckMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(PUBACK)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubAckMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self._mqttEncoder.encode(pubAckMsg)
        self._sendall(encodedMsg)

    def _handlePubAckMsg(self, mqttMessage):
        resultHandler = self._getResultHandler(mqttMessage)
        if(resultHandler):
            if(callable(resultHandler)): resultHandler(Result(mqttMessage.messageId, 1, None))
            resultHandler = None
            del self._resultHandlers[mqttMessage.messageId]
            
    def _handlePubRelMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(PUBCOMP)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubComMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self._mqttEncoder.encode(pubComMsg)
        self._sendall(encodedMsg)
        if(mqttMessage.messageId  in self._msgIdInbox):
            self._msgIdInbox.remove(mqttMessage.messageId)
    
    def _handlePubRecMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(PUBREL, 1)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubRelMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self._mqttEncoder.encode(pubRelMsg)
        self._sendall(encodedMsg)
    
    def _resetSock(self):
        self._disconnecting = 1
        self._log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Resetting connection due to socket error or connect timeout...')
        self._closeSocket()
     
    
    def _initSock(self):
        t = time.time()
#         if (self._sockInit is 0 and self._nextConnTry - t > 0): raise SocketError('Last connection failed. Waiting before retry.')
        waitFor = self._nextConnTry - t
        if (self._sockInit is 0 and waitFor > 0): 
            if(not self._waitingReconnect):
                self._waitingReconnect = 1
                self._log(MQTT_LOG_LEVEL_DEBUG, '[MqttClient]:: Last connection failed. Waiting  for %d seconds before retry...' % int(waitFor))
            return
        if (self._sockInit is 0 and waitFor <= 0):
            self._nextConnTry = t + self.reconnectTimer
            if(self._sock is not None):
                self._closeSocket()
            self._setSocketNConnect()
            self._sockInit = 1
            self._waitingReconnect = 0          
            

    # def _getCommonNameFromCertificate(self):
    #     certDer = self._sock.getpeercert(binary_form=True)
    #     if(certDer is not None):
    #         x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_ASN1, certDer)
    #         return x509.get_subject().commonName
    #     return None
    
    def _closeSocket(self):
        try:
            self._log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Closing socket...')
            if(self._sock):
                self._sock.close()
                self._sock = None
        except: 
            self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = _closeSocket][Exception]:: %s" % (traceback.print_exc()))
        finally:
            self._sockInit = 0
            self._connected = 0
            self._connecting = 0
            self._disconnecting = 0
            self._lastPingReqTime = time.time()
            self._lastPingRespTime = self._lastPingReqTime
            if(self._onDisconnectCallBack): self._onDisconnectCallBack()
    
    def _generateMessageId(self): 
        if self._messageId == MQTT_MAX_INT:
            self._messageId = 0
        self._messageId = self._messageId + 1
        return self._messageId
    
    def _processHandlersTimeout(self):
        for key in list(self._resultHandlers):
            value = self._resultHandlers[key]
            if((time.time() - value['time']) >= value['timeout']):
                resultHandler = value['handler']             
                if(resultHandler and callable(resultHandler)):
                    timeOutMsg = value['timeOutMsg']
                    resultHandler(Result(None, 0, (MQTT_ERROR_TIMEOUT, timeOutMsg)))
                    value['handler'] = None
                del self._resultHandlers[key]
                
    def _sendPingReq(self):
        fixedHeader = MqttFixedHeader(PINGREQ)
        pingReqMsg = self._mqttMsgFactory.message(fixedHeader)
        encodedMsg = self._mqttEncoder.encode(pingReqMsg)
        self._sendall(encodedMsg)