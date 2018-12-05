# -*- coding: utf-8 -*-
import sys
import os
import time
import json
import fcntl
import struct
import _thread
from threading import Thread, Event, RLock 
import socket
import OpenSSL.crypto
try:
    import ssl
    HAS_SSL = True
except:
    HAS_SSL = False

from .messages import MqttFixedHeader, MqttMsgFactory
from .decoder import MqttDecoder
from .encoder import MqttEncoder
from .result import Result
from .errors import *
from .constants import *


class MqttClient:

    def __init__(self, host, port, clientId, enableSsl=0, options={}):
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
        self.enableSsl= enableSsl
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
        self.__lastConnectTime = time.time()
        self.decoderErrorCount = 0
        self.__connectAckTimeoutCount = 0
        
        
    def process(self):
        try:
            if(not self.__disconnecting):
                self.connect()
                if(self.__sockInit):
                    mqttMsg = self.__receive()
                    if (mqttMsg): self.__handleMqttMessage(mqttMsg)
                    if ((self.__lastPingReqTime + (1 + MQTT_KEEP_ALIVE_TIMER_GRACE) * self.keepAliveTimer) < time.time()):
                        if (self.__lastPingRespTime is None):
                            self.disconnect()
                        else: 
                            self.__sendPingReq()
                            self.__lastPingReqTime = time.time()
                            self.__lastPingRespTime = None
                self.__processHandlersTimeout()
                if (self.__connecting and ((self.__lastConnectTime + CONNECT_ACK_TIMEOUT) < time.time())):
                    self.__log(MQTT_LOG_LEVEL_INFO, "[MqttClientError, method = process]::Connect Ack timed out. Reseting connection.")
                    self.__resetSock()
        except socket.error as msg:
            self.__resetSock()
            self.__log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = process][SocketError]:: %s" % (str(msg)))
        except MqttConnectError as msg:
            self.__log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = process][MqttConnectError]:: %s" % (str(msg)))
        except:
            self.__log(MQTT_LOG_LEVEL_ERROR, "[MqttClientError, method = process][Exception]:: %s %s" % (str(sys.exc_info()[0]), str(sys.exc_info()[1])))


    def provision(self, provId, provPin, timeout = 300):
        try:
            auth = None
            self.__initSock()
            if(self.__sockInit):
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
                provisionMsg = self.__mqttMsgFactory.message(fixedHeader, options, options)
                encodedMsg = self.__mqttEncoder.encode(provisionMsg)
                self.__sendall(encodedMsg)
                timeout = time.time() + timeout
                while(timeout > time.time()):
                    time.sleep(10)
                    mqttMsg = self.__receive()
                    if(mqttMsg and mqttMsg.fixedHeader.messageType == self.PROVACK):
                        auth = self.__getAuthInfoFromProvAckMsg(mqttMsg)
                        break
                return auth
        finally:
            self.__closeSocket()

    def connect(self):
        try:
            self.__initSock()
            if(self.__connecting is 0 and self.__sockInit):
                if(not self.__connected):
                    self.__connecting = 1
                    self.__log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Mqtt Connecting to %s:%s' % (self.host, str(self.port)))   
                    fixedHeader = MqttFixedHeader(CONNECT, qos=0, dup=0, retain=0)
                    connectMsg = self.__mqttMsgFactory.message(fixedHeader, self.options, self.options)
                    encodedMsg = self.__mqttEncoder.encode(connectMsg)
                    self.__sendall(encodedMsg)
        except socket.timeout:
            self.__connecting = 0
            self.__log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = connect][SocketTimeoutError]:: Socket timed out")
        except socket.error as msg:
            self.__resetSock()
            self.__log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = connect][SocketError]:: %s" % (str(msg)))
        except:
            self.__connecting = 0
            self.__log(MQTT_LOG_LEVEL_ERROR, "[MqttClientError, method = connect][Exception]:: %s %s" % (str(sys.exc_info()[0]), str(sys.exc_info()[1])))
    
    def disconnect(self):
        try:
            try:
                self.__disconnecting = 1
                if(not self.__connecting  and not self.__waitingReconnect and self.__sockInit):
                    fixedHeader = MqttFixedHeader(DISCONNECT, qos=0, dup=0, retain=0)
                    disConnectMsg = self.__mqttMsgFactory.message(fixedHeader)
                    encodedMsg = self.__mqttEncoder.encode(disConnectMsg)
                    self.__sendall(encodedMsg)
            except Exception as msg:
                self.__log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = __receive][%s]:: %s" % (msg.__class__.__name__ , str(msg)))
        finally:
            self.__resetSock()
    
    def publish(self, topic, payload, qos=MQTT_QOS0, dup=0, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT, retain=0, logging=1):
        if(not self.__connected or self.__connecting  or self.__waitingReconnect):
            raise MqttClientError("Cannot publish message as not connected.")
        self.__validateTopic(topic)
        self.__validateQos(qos)
        self.__validateResultHandler(resultHandler)
        self.__validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(PUBLISH, qos, dup=0, retain=0)
        messageId = 0
        if(qos > MQTT_QOS0): messageId = self.__generateMessageId()
        variableHeader = {'messageId': messageId, 'topic': str(topic)}
        publishMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader, payload)
        encodedMsg = self.__mqttEncoder.encode(publishMsg)
        if(logging):
            self.__log(MQTT_LOG_LEVEL_DEBUG, '[MqttClient]:: sending message:%s' % publishMsg.toString())
        if (qos > MQTT_QOS0 and messageId and resultHandler): 
            timeOutMsg = 'Publishing message %s to topic %s with qos %d timed out.' % (payload, topic, qos)
            self.__resultHandlers[messageId] = {'time':time.time(), 'timeout': resultHandlerTimeout, 'handler':resultHandler, 'timeOutMsg':timeOutMsg}
        self.__sendall(encodedMsg)      
        if(qos == MQTT_QOS0 and resultHandler): 
            resultHandler(Result(None, 1))  # immediately return messageId 0 in case of qos 0

        
    def subscribe(self, topic, qos, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT):
        if(not self.__connected or self.__connecting  or self.__waitingReconnect):
            raise MqttClientError("Cannot subscribe as not connected.")
        self.__validateTopic(topic)
        self.__validateQos(qos)
        self.__validateResultHandler(resultHandler)
        self.__validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(SUBSCRIBE, qos=1, dup=0, retain=0)
        messageId = self.__generateMessageId()
        variableHeader = {'messageId': messageId}
        subMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader, {'topic':topic, 'qos':qos})
        encodedMsg = self.__mqttEncoder.encode(subMsg)
        self.__log(MQTT_LOG_LEVEL_DEBUG, '[MqttClient]:: sending subscribe message: %s' % subMsg.toString())
        if(resultHandler):
            timeOutMsg = 'Subscribe to topic %s with qos %d timed out.' % (topic, qos)
            self.__resultHandlers[messageId] = {'time':time.time(), 'timeout': resultHandlerTimeout, 'handler':resultHandler, 'timeOutMsg':timeOutMsg}
        self.__sendall(encodedMsg)
                
    def unsubscribe(self, topics, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT):
        if(not self.__connected or self.__connecting  or self.__waitingReconnect):
            raise MqttClientError("Cannot unsubscribe as not connected.")
        self.__validateResultHandler(resultHandler)
        self.__validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(UNSUBSCRIBE, qos=1, dup=0, retain=0)
        messageId = self.__generateMessageId()
        variableHeader = {'messageId': messageId}
        if(isinstance(topics, str)):
            topics = [topics]
        if(isinstance(topics, list)):
            for topic in topics:
                self.__validateTopic(topic)
                unsubMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader, topics)
                encodedMsg = self.__mqttEncoder.encode(unsubMsg)
                self.__log(MQTT_LOG_LEVEL_DEBUG, '[MqttClient]:: sending unsubscribe message: %s' % unsubMsg.toString())
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
        if (len(topic) < MQTT_MAX_TOPIC_LEN + 1):
            pass
        else:
            raise ValueError('Topic length cannot be more than %d' % MQTT_MAX_TOPIC_LEN)
        
    def __validateQos(self, qos):
        if(not isinstance(qos, int) or qos < MQTT_QOS0 or qos > MQTT_QOS2):
            raise ValueError('Qos should be a between %d and %d.' % (MQTT_QOS0, self.MQTT_QOS2)) 
        
    def __validateRetain(self, retain):
        if (not isinstance(retain, int) or retain != 0 or retain != 1):
            raise ValueError('Retain can only be integer 0 or 1')
        
    def __validateTimeout(self, timeout):
        if (not isinstance(timeout, int) or timeout < 0 or timeout > MQTT_MAX_RESULT_HANDLER_TIMEOUT):
            raise ValueError('Timeout can only be integer between 0 and %d.' % MQTT_MAX_RESULT_HANDLER_TIMEOUT)
        
    def __validateResultHandler(self, resultHandler):
        if(resultHandler is not None and not callable(resultHandler)):            
            raise ValueError('Result Handler should be a callable object.') 
            
    def __log(self, level, msg):
        if(level <= self.__logLevel):
            if(self.__onDebugMessageCallBack):
                self.__onDebugMessageCallBack(level, msg)

    def __sendall(self, data):
        self.lock.acquire()
        try:
            if(data):
                try:
                    self.__sock.sendall(self.__str_to_unencoded_bytes(data))
                except socket.error as msg:                  
                    self.__resetSock()
                    raise socket.error(str("Socket error in send: %s. Connection reset." % (str(msg))))
        finally:
            self.lock.release()
      
    def __receive(self):
        self.lock.acquire()
        try:
            try:
                data = self.__sock.recv(MQTT_SOCKET_MAX_BYTES_READ)
                if (data is not None) and (len(data) > 0): 
                    mqttMsg = self.__mqttDecoder.decode(data)
                    self.decoderErrorCount = 0
                    if (mqttMsg):
                        self.__log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Received message:%s' % mqttMsg.toString())
                else:
                    mqttMsg = None
                return mqttMsg 
            except MqttDecoderError as msg:
                self.decoderErrorCount = self.decoderErrorCount + 1
                self.__log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = __receive][%s]:: %s" % (msg.__class__.__name__ , str(msg)))
                if (self.decoderErrorCount > MQTT_DECODER_ERROR_COUNT_FOR_SOCKET_RESET):
                    self.__log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = __receive]:: Resetting socket as MqttDecoderError count exceeded %s" % (str(MQTT_DECODER_ERROR_COUNT_FOR_SOCKET_RESET)))
                    self.decoderErrorCount = 0
                    self.__resetSock()                    
            except socket.timeout:
                self.__log(MQTT_LOG_LEVEL_DEBUG, "[MqttClient, method = __receive][Socket time out. No data to receive...]")
                pass
            except (MqttFrameError, socket.error) as msg:
                if 'timed out' in msg.message.lower():
                    # Hack as ssl library does not throw timeout error
                    pass
                else:
                    self.__resetSock()
                    self.__log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientError, method = __receive][%s]:: %s" % (msg.__class__.__name__ , str(msg)))
        finally:
            self.lock.release() 

    def __str_to_unencoded_bytes(self, s):
        """Convert a string to raw bytes without encoding"""
        outlist = []
        for cp in s:
            num = ord(cp)
            if num < 255:
                outlist.append(struct.pack('B', num))
            elif num < 65535:
                outlist.append(struct.pack('>H', num))
            else:
                b = (num & 0xFF0000) >> 16
                H = num & 0xFFFF
                outlist.append(struct.pack('>bH', b, H))
        return b''.join(outlist)
  
    def __handleMqttMessage(self, mqttMessage):
        self.__lastPingRespTime = time.time()
        msgType = mqttMessage.fixedHeader.messageType
        if msgType == CONNACK:
            self.__handleConnAckMsg(mqttMessage)
        elif msgType == PUBLISH:
            self.__handlePublishMsg(mqttMessage)
        elif msgType == SUBACK:
            self.__handleSubAck(mqttMessage)
        elif msgType == UNSUBACK:
            self.__handleUnSubAck(mqttMessage)
        elif msgType == PUBACK:
            self.__handlePubAckMsg(mqttMessage)
        elif msgType == PUBREC:
            self.__handlePubRecMsg(mqttMessage)
        elif msgType == PUBCOMP:
            self.__onPublish(mqttMessage)
        elif msgType == PUBREL:
            self.__handlePubRelMsg(mqttMessage)
        elif msgType == PINGRESP:
            self.__lastPingRespTime = time.time()
        elif msgType in [CONNECT, PROVACK, SUBSCRIBE, UNSUBSCRIBE, PINGREQ]:
            pass  # Client will not receive these messages
        else:
            raise MqttClientError('[MqttClientError, method = __handleMqttMessage]:: Unknown message type.') 
    
    def __getResultHandler(self, mqttMessage):
        resultHandler = None
        resultHandlerDict = self.__resultHandlers.get(mqttMessage.messageId)
        if(resultHandlerDict):
            resultHandler = resultHandlerDict.get('handler')
        return resultHandler
    
    def __handleSubAck(self, mqttMessage):
        resultHandler = self.__getResultHandler(mqttMessage)
        if(resultHandler):
            resultHandler(Result(mqttMessage, 1))
            del self.__resultHandlers[mqttMessage.messageId]
    
    def __handleUnSubAck(self, mqttMessage):
        resultHandler = self.__getResultHandler(mqttMessage)
        if(resultHandler):
            resultHandler(Result(mqttMessage, 1))
            del self.__resultHandlers[mqttMessage.messageId]
    
    def __onPublish(self, mqttMessage):
        resultHandler = self.__getResultHandler(mqttMessage)
        if(resultHandler):
            resultHandler(Result(mqttMessage, 1))
            del self.__resultHandlers[mqttMessage.messageId]
    
    def __handleConnAckMsg(self, mqttMessage):
        self.__connecting = 0
        connectReturnCode = mqttMessage.connectReturnCode
        if(connectReturnCode == CONNECTION_ACCEPTED):
            self.__connected = 1
            self.__lastConnectTime = time.time()
            self.__log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Connected to %s:%s' % (self.host, str(self.port)))   
            if(self.__onConnectCallBack): self.__onConnectCallBack(self)  
        else:
            self.__handleProvisionAndConnectAckCode("Connection", connectReturnCode)

            
    def __getAuthInfoFromProvAckMsg(self, mqttMessage):
        provisionReturnCode = mqttMessage.provisionReturnCode
        if(provisionReturnCode == CONNECTION_ACCEPTED):
            payload = mqttMessage.payload
            clientId=payload[0:36]
            authToken = payload[37:]
            return (clientId, authToken)
        else:
            self.__handleProvisionAndConnectAckCode("Provisioning", provisionReturnCode)
            
            
    def __handleProvisionAndConnectAckCode(self, type, code):
        msg =''
        if(code == CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION):
            msg = '[MqttClient]:: %s refused unacceptable mqtt protocol version.' % type
        elif(code == CONNECTION_REFUSED_IDENTIFIER_REJECTED):
            msg =  '[MqttClient]:: %s refused client identifier rejected.' % type
        elif(code == CONNECTION_REFUSED_SERVER_UNAVAILABLE): 
            msg =  '[MqttClient]:: %s refused server unavailable. Waiting ...' % type
            self.__log(MQTT_LOG_LEVEL_DEBUG, msg)
            return # Not an error just wait for server
        elif(code == CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD):  
            msg = '[MqttClient]:: %s refused bad username or password.' % type
        elif(code == CONNECTION_REFUSED_NOT_AUTHORIZED):  
            msg = '[MqttClient]:: %s refused not authorized.' % type
        raise MqttConnectError(msg) # Error should be bubbled up
    
    def __handlePublishMsg(self, mqttMessage):
        if(mqttMessage.fixedHeader.qos > MQTT_QOS1): 
            if(mqttMessage.messageId not in self.__msgIdInbox):
                self.__msgIdInbox.append(mqttMessage.messageId)
        if(self.__onMessageCallBack):
            self.__onMessageCallBack(mqttMessage)
        if(mqttMessage.fixedHeader.qos == MQTT_QOS1):
            self.__sendPubAckMsg(mqttMessage)

    def __sendPubAckMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(PUBACK)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubAckMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self.__mqttEncoder.encode(pubAckMsg)
        self.__sendall(encodedMsg)

    def __handlePubAckMsg(self, mqttMessage):
        resultHandler = self.__getResultHandler(mqttMessage)
        if(resultHandler):
            resultHandler(Result(mqttMessage.messageId, 1, None))
            resultHandler = None
            del self.__resultHandlers[mqttMessage.messageId]
            
    def __handlePubRelMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(PUBCOMP)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubComMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self.__mqttEncoder.encode(pubComMsg)
        self.__sendall(encodedMsg)
        if(mqttMessage.messageId  in self.__msgIdInbox):
            self.__msgIdInbox.remove(mqttMessage.messageId)
    
    def __handlePubRecMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(PUBREL, 1)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubRelMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self.__mqttEncoder.encode(pubRelMsg)
        self.__sendall(encodedMsg)
    
    def __resetSock(self):
        self.__disconnecting = 1
        self.__log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Resetting connection due to socket error or connect timeout...')
        self.__closeSocket()
     
    
    def __initSock(self):
        t = time.time()
#         if (self.__sockInit is 0 and self.__nextConnTry - t > 0): raise SocketError('Last connection failed. Waiting before retry.')
        waitFor = self.__nextConnTry - t
        if (self.__sockInit is 0 and waitFor > 0): 
            if(not self.__waitingReconnect):
                self.__waitingReconnect = 1
            self.__log(MQTT_LOG_LEVEL_DEBUG, '[MqttClient]:: Last connection failed. Waiting  for %d seconds before retry...' % int(waitFor))
            return
        if (self.__sockInit is 0 and waitFor <= 0):
            self.__nextConnTry = t + self.reconnectTimer
            if(self.__sock is not None):
                self.__closeSocket()
                self.__log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Opening socket to %s:%s' % (self.host, str(self.port)))
            self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.__sock.settimeout(MQTT_SOCKET_TIMEOUT)         
            if(self.enableSsl):
                self.__sock = ssl.wrap_socket(self.__sock, cert_reqs=ssl.CERT_NONE)
                self.__sock.connect((self.host, self.port))
                commonName = self.__getCommonNameFromCertificate()
                domain = self.host.split(".")[-2:]
                domain = ".".join(domain)
                if(commonName == domain):
                    self.__sockInit = 1
                    self.__waitingReconnect = 0
                    self.__log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Socket opened to %s:%s' % (self.host, str(self.port)))
                else:
                    self.__log(MQTT_LOG_LEVEL_ERROR, '[MqttClient]:: Ssl certificate error. Host %s does not match host %s provide in certificate.' % (self.host, commonName))  
            else:
                self.__sock.connect((self.host, self.port))
                self.__sockInit = 1
                self.__waitingReconnect = 0
                self.__log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Socket opened to %s:%s' % (self.host, str(self.port))) 
            
    def __getCommonNameFromCertificate(self):
        certDer = self.__sock.getpeercert(binary_form=True)
        if(certDer is not None):
            x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_ASN1, certDer)
            return x509.get_subject().commonName
        return None
    
    def __closeSocket(self):
        try:
            self.__log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Closing socket...')
            if(self.__sock):
                self.__sock.close()
                self.__sock = None
        except:
            self.__log(INSTAMSG_LOG_LEVEL_ERROR, '[MqttClient]:: Unexpected Error while closing socket...')  
        finally:
            self.__sockInit = 0
            self.__connected = 0
            self.__connecting = 0
            self.__disconnecting = 0
            self.__lastPingReqTime = time.time()
            self.__lastPingRespTime = self.__lastPingReqTime
            if(self.__onDisconnectCallBack): self.__onDisconnectCallBack()
    
    def __generateMessageId(self): 
        if self.__messageId == MQTT_MAX_INT:
            self.__messageId = 0
        self.__messageId = self.__messageId + 1
        return self.__messageId
    
    def __processHandlersTimeout(self):
        for key, value in self.__resultHandlers.items():
            if((time.time() - value['time']) >= value['timeout']):
                resultHandler = value['handler']
                if(resultHandler):
                    timeOutMsg = value['timeOutMsg']
                    resultHandler(Result(None, 0, (MQTT_ERROR_TIMEOUT, timeOutMsg)))
                    value['handler'] = None
                del self.__resultHandlers[key]
                
    def __sendPingReq(self):
        fixedHeader = MqttFixedHeader(PINGREQ)
        pingReqMsg = self.__mqttMsgFactory.message(fixedHeader)
        encodedMsg = self.__mqttEncoder.encode(pingReqMsg)
        self.__sendall(encodedMsg)