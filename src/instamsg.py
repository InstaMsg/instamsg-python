# -*- coding: utf-8 -*-
import time
import socket
import sys
import os

####InstaMsg ###############################################################################
INSTAMSG_MAX_BYTES_IN_MSG = 10240
INSTAMSG_HOST = "localhost"
INSTAMSG_PORT = 1883
INSTAMSG_PORT_SSL = 8883
INSTAMSG_HTTP_HOST = 'api.instamsg.io'
INSTAMSG_HTTP_PORT = 80
INSTAMSG_HTTPS_PORT = 443
INSTAMSG_RESULT_HANDLER_TIMEOUT = 10    

class InstaMsg:
    def __init__(self, clientId, autKey, connectHandler, disConnectHandler, options={}):
        if(not callable(connectHandler)): raise ValueError('connectHandler should be a callable object.')
        if(not callable(disConnectHandler)): raise ValueError('disConnectHandler should be a callable object.')
        self.clientId = clientId
        self.autKey = autKey    
        self.options = options
        
        
        self.__enableTcp = options.get('tcp') or 1
        self.__defaultReplyTimeout = INSTAMSG_RESULT_HANDLER_TIMEOUT
        self.__msgHandlers ={}
        self.__sendMsgHandlers = {}  # {handlerId:{time:122334,handler:replyHandler, timeout:10}}
        if(options.get('ssl')): 
            port = INSTAMSG_PORT_SSL 
            httpPort = INSTAMSG_HTTPS_PORT
        else: 
            port = INSTAMSG_PORT
            httpPort = INSTAMSG_HTTP_PORT
        if(self.__enableTcp):
            clientIdAndUsername = self.__getClientIdAndUsername(clientId)
            mqttoptions = self.__mqttClientOptions(clientIdAndUsername[1], autKey)
            self.__mqttClient = MqttClient(INSTAMSG_HOST, port, clientIdAndUsername[0], mqttoptions)
            self.__mqttClient.onConnect(connectHandler)
            self.__mqttClient.onDisconnect(disConnectHandler)
            self.__mqttClient.onMessage(self.__handleMessage)
            self.__mqttClient.connect()
        else:
            self.__mqttClient = None
        self.__httpClient = HTTPClient('InstaMsg', INSTAMSG_HTTP_HOST, httpPort)
        
    
    def process(self):
        self.__mqttClient.process()
    
    def close(self):
        try:
            self.__mqttClient.disconnect()
            self.__sendMsgHandlers = None
            self.__msgHandlers = None
            self.__subscribers = None
            return 1
        except:
            return -1
    
    def publish(self, topic, msg, qos=0, dup=0, resultHandler=None, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        if(topic):
            self.__mqttClient.publish(topic, msg, qos, dup, resultHandler, timeout)
        else: raise ValueError("Topic cannot be null or empty string.")
    
    def subscribe(self, topic, qos, msgHandler, resultHandler, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        if(self.__mqttClient):
            if(not callable(msgHandler)): raise ValueError('msgHandler should be a callable object.')
            if(topic and qos):
                self.__msgHandlers[topic] = msgHandler
                self.__mqttClient.subscribe(topic, qos, resultHandler, timeout)
        else:
            raise InstaMsgSubError("Cannot subscribe as TCP is not enabled. Two way messaging only possible on TCP and not HTTP")
            

    def unsubscribe(self, topics, resultHandler=None, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        if(self.__mqttClient):
            self.__mqttClient.unsubscribe(topics, resultHandler, timeout)
        else:
            raise InstaMsgUnSubError("Cannot unsubscribe as TCP is not enabled. Two way messaging only possible on TCP and not HTTP")
    
    def send(self, topic, msg, replyHandler, timeout):
        try:
            handlerId = 0
            pass
        except Exception, e:
            if(self.__sendMsgHandlers.has_key(handlerId)):
                del self.__sendMsgHandlers[handlerId]
            raise Exception(str(e))
        
    def sendFile(self):
        pass
    
    def logger(self):
        return self.logger
    
    def __onConnect(self):
        print "Connected"
    
    def __handleMessage(self, mqttMsg):
        msg = Message(mqttMsg.payload)
        msg.id = mqttMsg.messageId
        msg.dup = mqttMsg.fixedHeader.dup
        msg.qos = mqttMsg.fixedHeader.qos
        msgHandler = self.__msgHandlers.get(mqttMsg.topic)
        if(msgHandler):
            msgHandler(mqttMsg)
    
    def __mqttClientOptions(self, username, password):
        options = {}
        options['hasUserName'] = 1
        options['hasPassword'] = 1
        options['username'] = username
        options['password'] = password
        return options
        
    def __getClientIdAndUsername(self, clientId):
        errMsg = 'clientId is not a valid uuid e.g. cbf7d550-7204-11e4-a2ad-543530e3bc65'
        if(clientId is None): raise ValueError('clientId cannot be null.')
        if(len(clientId) != 36): raise ValueError(errMsg)
        c = clientId.split('-')
        if(len(c) != 5): raise ValueError(errMsg)
        cId = '-'.join(c[0:4])
        pswd = c[4 ]
        if(len(pswd) != 12): raise ValueError(errMsg)
        return (cId, pswd)
    
    def __parseJson(self,jsonString):
        return eval(jsonString) #Hack as not implemented Json Library
    

class Result:
    def __init__(self, result, failed=1, cause=None):
        self.result = result
        self.failed = 1
        self.cause = cause
        
    def result(self):
        return self.result
    
    def failed(self):
        return self.failed
    
    def succeeded(self):
        return not self.failed
    
    def cause(self):
        return self.cause


class Message:
    def __init__(self, content, options={}):
        self.topic = None
        self.content = content
        self.id = options.get('id') or 0
        self.dup = options.get('dup') or 0
        self.qos = options.get('qos') or 0
        self.retain = options.get('retain') or 0
    
    def qos(self):
        return self.qos
    
    def isDublicate(self):
        return self.dup
    
    def content(self):
        return self.content
    
    def setTopic(self, topic):
        self.topic = topic
    
    def topic(self):
        return self.topic
    
    def reply(self, msg, replyHandler, timeout):
        pass
    
    def fail(self, errorCode, errorMsg):
        pass
    
    def sendFile(self, fileName, resultHandler, timeout):
        pass
    
    def toString(self):
        return ('[ id=%s, topic=%s, content=%s, qos=%s, dup=%s, retain=%s]') % (str(self.id), self.topic, self.content, str(self.qos), str(self.dup), str(self.retain))
    

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

MQTT_KEEPALIVE_SECONDS = 60
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

class MqttClient:
    def __init__(self, host, port, clientId, options={}):
        if(not clientId):
            raise ValueError('clientId cannot be null.')
        if(not host):
            raise ValueError('host cannot be null.')
        if(not port):
            raise ValueError('port cannot be null.')
        self.host = host
        self.port = port
        self.clientId = clientId
        self.options = options
        self.options['clientId'] = clientId
        self.__debug = options.get('debug') or 0
        self.__cleanSession = 1;
        self.__sock = None
        self.__sockInit = 0
        self.__connected = 0
        self.__connecting = 0
        self.__nextConnTry = time.time()
        self.__nextPingReqTime = time.time()
        self.__lastPingRespTime = self.__nextPingReqTime
        self.__mqttMsgFactory = MqttMsgFactory()
        self.__mqttEncoder = MqttEncoder()
        self.__mqttDecoder = MqttDecoder()
        self.__messageId = 0
        self.__onDisconnectCallBack = None
        self.__onConnectCallBack = None
        self.__onMessageCallBack = None
        self.__onDebugMessageCallBack = None
        self.__msgIdInbox = []
        self.__resultHandlers = {}
               
    def process(self):
        try:
            self.connect()
            if(self.__sockInit):
                self.__receive()
                if (self.__nextPingReqTime - time.time() <= 0):
                    if (self.__nextPingReqTime - self.__lastPingRespTime > MQTT_KEEPALIVE_SECONDS):
#                         self.disconnect()
                        pass
                    else: self.__sendPingReq()
#         except SocketTimeoutError:
#             pass
#         except SocketError, msg:
        except socket.timeout:
            pass
        except socket.error, msg:
            self.__resetInitSockNConnect()
            if (self.__debug):
                self.__log("InstaMsg SocketError in process: %s" % (str(msg)))
        except:
            if (self.__debug):
                self.__log("InstaMsg Unknown Error in process: %s %s" % (str(sys.exc_info()[0]), str(sys.exc_info()[1])))
    
    def connect(self):
        if(self.__connecting == 0):
            self.__connecting = 1
            self.__initSock()
            if(not self.__connected):
                self.__log('MqttClient connecting to %s:%s' % (self.host, str(self.port)))   
                fixedHeader = MqttFixedHeader(CONNECT, qos=0, dup=0, retain=0)
                connectMsg = self.__mqttMsgFactory.message(fixedHeader, self.options, self.options)
                encodedMsg = self.__mqttEncoder.ecode(connectMsg)
                self.__sendall(encodedMsg)
    
    def disconnect(self):
        fixedHeader = MqttFixedHeader(DISCONNECT, qos=0, dup=0, retain=0)
        disConnectMsg = self.__mqttMsgFactory.message(fixedHeader)
        encodedMsg = self.__mqttEncoder.ecode(disConnectMsg)
        self.__sendall(encodedMsg)
        self.__closeSocket()
        self.__resetInitSockNConnect()
        if(self.__onDisconnectCallBack): self.__onDisconnectCallBack()
    
    def publish(self, topic, payload, qos=MQTT_QOS0, retain=0, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT):
        self.__validateTopic(topic)
        self.__validateQos(qos)
        self.__validateResultHandler(resultHandler)
        self.__validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(PUBLISH, qos=MQTT_QOS0, dup=0, retain=0)
        messageId = 0
        if(qos > MQTT_QOS0): messageId = self.__generateMessageId()
        variableHeader = {'messageId': messageId, 'topic': str(topic)}
        publishMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader, payload)
        encodedMsg = self.__mqttEncoder.ecode(publishMsg)
        self.__sendall(encodedMsg)
        self.__validateResultHandler(resultHandler)
        if(qos == MQTT_QOS0 and resultHandler): 
            resultHandler(0) #immediately return messageId 0 in case of qos 0
        elif (qos > MQTT_QOS0 and messageId and resultHandler): 
            self.__resultHandlers[messageId] = {'time':time.time(), 'timeout': resultHandlerTimeout, 'handler':resultHandler}
                
        
    def subscribe(self, topic, qos, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT):
        self.__validateTopic(topic)
        self.__validateQos(qos)
        self.__validateResultHandler(resultHandler)
        self.__validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(SUBSCRIBE, qos=1, dup=0, retain=0)
        messageId = self.__generateMessageId()
        variableHeader = {'messageId': messageId}
        subMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader, {'topic':topic, 'qos':qos})
        encodedMsg = self.__mqttEncoder.ecode(subMsg)
        if(resultHandler):
            self.__resultHandlers[messageId] = {'time':time.time(), 'timeout': resultHandlerTimeout, 'handler':resultHandler}
        self.__sendall(encodedMsg)
                
    def unsubscribe(self, topics, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT):
            self.__validateResultHandler(resultHandler)
            self.__validateTimeout(resultHandlerTimeout)
            fixedHeader = MqttFixedHeader(UNSUBSCRIBE, qos=1, dup=0, retain=0)
            messageId = self.__generateMessageId()
            variableHeader = {'messageId': messageId}
            if(isinstance(topics, str)):
                topics = [topics]
            for topic in topics:
                self.__validateTopic(topic)
            if(isinstance(topics, list)):
                unsubMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader, topics)
                encodedMsg = self.__mqttEncoder.ecode(unsubMsg)
                if(resultHandler):
                    self.__resultHandlers[messageId] = {'time':time.time(), 'timeout': resultHandlerTimeout, 'handler':resultHandler}
                self.__sendall(encodedMsg)
                return messageId
            else:   raise TypeError('Topics should be an instance of string or list.') 
    
    def onConnect(self, callback):
        if(callable(callback)):
            self.__onConnectCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.')
    
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
            raise ValueError('Qos should be a between %d and %d.' % (MQTT_QOS0, MQTT_QOS2)) 
        
    def __validateRetain(self, retain):
        if (not isinstance(retain, int) or retain != 0 or retain != 1):
            raise ValueError('Retain can only be integer 0 or 1')
        
    def __validateTimeout(self, timeout):
        if (not isinstance(timeout, int) or timeout < 0 or timeout > MQTT_MAX_RESULT_HANDLER_TIMEOUT):
            raise ValueError('Timeout can only be integer between 0 and %d.' % MQTT_MAX_RESULT_HANDLER_TIMEOUT)
        
    def __validateResultHandler(self, resultHandler):
        if(resultHandler is not None and not callable(resultHandler)):            
            raise ValueError('Result Handler should be a callable object.') 
            
    def __log(self, msg):
        if(self.__onDebugMessageCallBack):
            self.__onDebugMessageCallBack(msg)

    def __sendall(self, data):
        if(data):
            self.__log('EncodedMsg %s' % (data)) 
            self.__sock.sendall(data)
            self.__log('EncodedMsg11 %s' % (data)) 
            
    def __receive(self):
        try:
            data = self.__sock.recv(MAX_BYTES_MDM_READ)
            if data: 
                mqttMsg = self.__mqttDecoder.decode(data)
            else:
                mqttMsg = None
            if (mqttMsg):
                if (self.__debug):
                    self.__log('MqttClient:: Received message:%s' % mqttMsg.toString())
                self.__handleMqttMessage(mqttMsg) 
        except MqttFrameError:
            self.__resetInitSockNConnect()
        except MqttDecoderError:
            pass
#         except SocketTimeoutError:
#             pass
        except socket.timeout:
            pass
            
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
            self.__onPublish(mqttMessage)
        elif msgType == PUBREC:
            self.__handlePubRecMsg(mqttMessage)
        elif msgType == PUBCOMP:
            self.__onPublish(mqttMessage)
        elif msgType == PUBREL:
            self.__handlePubRelMsg(mqttMessage)
        elif msgType == PINGRESP:
            self.__lastPingRespTime = time.time()
        elif msgType in [CONNECT, SUBSCRIBE, UNSUBSCRIBE, PINGREQ]:
            pass  # Client will not receive these messages
        else:
            raise MqttEncoderError('MqttEncoder: Unknown message type.') 
    
    def __handleSubAck(self, mqttMessage):
        resultHandler = self.__resultHandlers.get(mqttMessage.messageId).get('handler')
        if(resultHandler):
            resultHandler(1)
            del self.__resultHandlers[mqttMessage.messageId]
    
    def __handleUnSubAck(self, mqttMessage):
        resultHandler = self.__resultHandlers.get(mqttMessage.messageId).get('handler')
        if(resultHandler):
            resultHandler(1)
            del self.__resultHandlers[mqttMessage.messageId]
    
    def __onPublish(self, mqttMessage):
        resultHandler = self.__resultHandlers.get(mqttMessage.messageId).get('handler')
        if(resultHandler):
            resultHandler(1)
            del self.__resultHandlers[mqttMessage.messageId]
    
    def __handleConnAckMsg(self, mqttMessage):
        connectReturnCode = mqttMessage.connectReturnCode
        if(connectReturnCode == CONNECTION_ACCEPTED):
            self.__connected = 1
            self.__connecting = 0
            self.__log('MqttClient connected to %s:%s' % (self.host, str(self.port)))  
            if(self.__onConnectCallBack): self.__onConnectCallBack(self)  
        elif(connectReturnCode == CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION):
            self.__log("CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION")
        elif(connectReturnCode == CONNECTION_REFUSED_IDENTIFIER_REJECTED):  
            self.__log("CONNECTION_REFUSED_IDENTIFIER_REJECTED")
        elif(connectReturnCode == CONNECTION_REFUSED_SERVER_UNAVAILABLE):  
            self.__log("CONNECTION_REFUSED_SERVER_UNAVAILABLE")
        elif(connectReturnCode == CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD):  
            self.__log("CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD")
        elif(connectReturnCode == CONNECTION_REFUSED_NOT_AUTHORIZED):  
            self.__log("CONNECTION_REFUSED_NOT_AUTHORIZED")
    
    def __handlePublishMsg(self, mqttMessage):
        if(mqttMessage.fixedHeader.qos > MQTT_QOS1): 
            if(mqttMessage.messageId not in self.__msgIdInbox):
                self.__msgIdInbox.append(mqttMessage.messageId)
        if(self.__onMessageCallBack):
            self.__onMessageCallBack(mqttMessage)
            
    def __handlePubRelMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(PUBCOMP)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubComMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self.__mqttEncoder.ecode(pubComMsg)
        self.__sendall(encodedMsg)
        self.__msgIdInbox.remove(mqttMessage.messageId)
    
    def __handlePubRecMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(PUBREL)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubRelMsg = self.__mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self.__mqttEncoder.ecode(pubRelMsg)
        self.__sendall(encodedMsg)
    
    def __resetInitSockNConnect(self):
        self.__sockInit = 0
        self.__connected = 0
        self.__connecting = 0
    
    def __initSock(self):
        t = time.time()
#         if (self.__sockInit is 0 and self.__nextConnTry - t > 0): raise SocketError('Last connection failed. Waiting before retry.')
        if (self.__sockInit is 0 and self.__nextConnTry - t > 0): raise socket.error('Last connection failed. Waiting before retry.')
        if (self.__sockInit is 0 and self.__nextConnTry - t <= 0):
            self.__nextConnTry = t + 90
            if(self.__sock is not None):
                self.__log('MqttClient closing socket...')
                self.__closeSocket()
                self.__log('MqttClient opening socket to %s:%s' % (self.host, str(self.port)))
#             self.__sock = Socket(10, self.keepAlive)
            self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.__sock.settimeout(10)
            self.__sock.connect((self.host, self.port))
            self.__sockInit = 1
            self.__log('MqttClient socket opened to %s:%s' % (self.host, str(self.port)))   
    
    def __closeSocket(self):
        try:
            if(self.__sock):
                self.__sock.close()
                self.__sock = None
        except:
            pass 
    
    def __generateMessageId(self): 
        if self.__messageId == MQTT_MAX_INT:
            self.__messageId = 0
        self.__messageId = self.__messageId + 1
        return self.__messageId
    
    def __processHandlerTimeouts(self):
        for key, value in self.__resultHandlers.items():
            if((time.time() - value['time']) >= value['timeout']):
                value['handler'] = None
                del self.__resultHandlers[key]
                
    def __sendPingReq(self):
        fixedHeader = MqttFixedHeader(PINGREQ)
        pingReqMsg = self.__mqttMsgFactory.message(fixedHeader)
        encodedMsg = self.__mqttEncoder.ecode(pingReqMsg)
        self.__sendall(encodedMsg)
        self.__nextPingReqTime = time.time() + MQTT_KEEPALIVE_SECONDS
    
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
                if (self.__fixedHeader.messageType == PUBLISH and not self.__variableHeader):
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
        self.__variableHeader['topicLength']= None
        self.__variableHeader['messageId'] = None
        self.__variableHeader['topic'] = None
        

    def __decodeVariableHeader(self):  
        if self.__fixedHeader.messageType in [CONNECT, SUBSCRIBE, UNSUBSCRIBE, PINGREQ]:
            self.__state = self.DISCARDING_MESSAGE
            self.__error = ('MqttDecoder: Client cannot receive CONNECT, SUBSCRIBE, UNSUBSCRIBE, PINGREQ message type.') 
        elif self.__fixedHeader.messageType == CONNACK:
            if(self.__fixedHeader.remainingLength != 2):
                self.__state = self.BAD
                self.__error = ('MqttDecoder: Mqtt CONNACK message should have remaining length 2 received %s.' % self.__fixedHeader.remainingLength) 
            elif(len(self.__data) < 2):
                pass  # let for more bytes come
            else:
                self.__getByteStr()  # discard reserved byte
                self.__variableHeader['connectReturnCode'] = ord(self.__getByteStr())
                self.__state = self.MESSAGE_READY
        elif self.__fixedHeader.messageType == SUBACK:
            messageId = self.__decodeMsbLsb()
            if(messageId is not None):
                self.__variableHeader['messageId'] = messageId
                self.__state = self.READING_PAYLOAD
        elif self.__fixedHeader.messageType in [UNSUBACK, PUBACK, PUBREC, PUBCOMP, PUBREL]:
            messageId = self.__decodeMsbLsb()
            if(messageId is not None):
                self.__variableHeader['messageId'] = messageId
                self.__state = self.MESSAGE_READY
        elif self.__fixedHeader.messageType == PUBLISH:
            if(self.__variableHeader['topic'] is None):
                self.__decodeTopic()
            if (self.__variableHeader['topic'] is not None and self.__variableHeader['messageId'] is None):
                self.__variableHeader['messageId'] = self.__decodeMsbLsb()
            if (self.__variableHeader['topic'] is not None and self.__variableHeader['messageId'] is not None):
                self.__state = self.READING_PAYLOAD
        elif self.__fixedHeader.messageType in [PINGRESP, DISCONNECT]:
            self.__mqttMsg = self.__msgFactory.message(self.__fixedHeader)
            self.__state = self.MESSAGE_READY
        else:
            self.__state = self.DISCARDING_MESSAGE
            self.__error = ('MqttDecoder: Unrecognised message type.') 
            
    def __decodePayload(self, bytesRemaining):
        paloadBytes = self.__getNBytesStr(bytesRemaining)
        if(paloadBytes is not None):
            if self.__fixedHeader.messageType == SUBACK:
                grantedQos = []
                numberOfBytesConsumed = 0
                while (numberOfBytesConsumed < bytesRemaining):
                    qos = int(ord(paloadBytes[numberOfBytesConsumed]) & 0x03)
                    numberOfBytesConsumed = numberOfBytesConsumed + 1
                    grantedQos.append(qos)
                self.__payload = grantedQos
                self.__state = self.MESSAGE_READY
            elif self.__fixedHeader.messageType == PUBLISH:
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
        if (intMsbLsb < 0 or intMsbLsb > MQTT_MAX_INT):
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
        if msgType == CONNECT:
            return self.__encodeConnectMsg(mqttMessage) 
        elif msgType == CONNACK:
            return self.__encodeConnAckMsg(mqttMessage)
        elif msgType == PUBLISH:
            return self.__encodePublishMsg(mqttMessage)
        elif msgType == SUBSCRIBE:
            return self.__encodeSubscribeMsg(mqttMessage)
        elif msgType == UNSUBSCRIBE:
            return self.__encodeUnsubscribeMsg(mqttMessage)
        elif msgType == SUBACK:
            return self.__encodeSubAckMsg(mqttMessage)
        elif msgType in [UNSUBACK, PUBACK, PUBREC, PUBCOMP, PUBREL]:
            return self.__encodeFixedHeaderAndMessageIdOnlyMsg(mqttMessage)
        elif msgType in [PINGREQ, PINGRESP, DISCONNECT]:
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
            encodedVariableHeader = self.__encodeIntShort(len(mqttConnectMessage.protocolName)) + mqttConnectMessage.protocolName + chr(mqttConnectMessage.version) + chr(connectFlagsByte) + self.__encodeIntShort(mqttConnectMessage.keepAliveTimeSeconds)
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
            return self.__encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload) + encodedVariableHeader + str(mqttPublishMsg.payload)
             
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
            if msgType == UNSUBACK: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttUnsubAckMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == PUBACK: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubAckMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == PUBREC: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubRecMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == PUBCOMP: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubCompMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == PUBREL: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubRelMsg.__name__, mqttMessage.__class__.__name__))
    
    def __encodeFixedHeaderOnlyMsg(self, mqttMessage):
        msgType = mqttMessage.fixedHeader.messageType
        if(isinstance(mqttMessage, MqttPingReqMsg) or isinstance(mqttMessage, MqttPingRespMsg) or isinstance(mqttMessage, MqttDisconnetMsg)):
            return self.__encodeFixedHeader(mqttMessage.fixedHeader, 0, None)
        else:
            if msgType == PINGREQ: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPingReqMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == PINGRESP: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPingRespMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == DISCONNECT: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttDisconnetMsg.__name__, mqttMessage.__class__.__name__))
    
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
        i = 0
        while 1:
            digit = num % 128
            num /= 128
            if (num > 0):
                digit |= 0x80
            else:
                if(i > 0):
                    break
            i = i + 1
            remainingLength += chr(digit) 
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
        return 'fixedHeader=[messageType=%s, dup=%d, qos=%d, retain=%d, remainingLength=%d]' %(str(self.messageType), self.dup, self.qos, self.retain, self.remainingLength)
        
class MqttMsg:
    def __init__(self, fixedHeader, variableHeader=None, payload=None):
        self.fixedHeader = fixedHeader
        self.variableHeader = variableHeader
        self.payload = payload
        
    def toString(self):
        return '%s[[%s] [variableHeader= %s] [payload= %s]]' %(self.__class__.__name__, self.fixedHeader.toString(),str(self.variableHeader), self.payload)
        

class MqttConnectMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader, payload):
        MqttMsg.__init__(self, fixedHeader, variableHeader, payload)
        self.fixedHeader = fixedHeader
        self.protocolName = MQTT_PROTOCOL_NAME
        self.version = MQTT_PROTOCOL_VERSION
        self.hasUserName = variableHeader.get('hasUserName') or 0
        self.hasPassword = variableHeader.get('hasPassword') or 0
        self.isWillRetain = variableHeader.get('isWillRetain') or 0
        self.willQos = variableHeader.get('willQos') or 0
        self.isWillFlag = variableHeader.get('isWillFlag') or 0
        self.isCleanSession = variableHeader.get('isCleanSession') or 1
        self.keepAliveTimeSeconds = variableHeader.get('keepAliveTimeSeconds') or MQTT_KEEPALIVE_SECONDS
        self.clientId = payload.get('clientId') or ''
        self.username = payload.get('username') or ''
        self.password = payload.get('password') or ''
        self.willTopic = payload.get('willTopic') or ''
        self.willMessage = payload.get('willMessage') or ''
        
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
        MqttMsg.__init__(self, fixedHeader,variableHeader)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        
class MqttPubRecMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader):
        MqttMsg.__init__(self, fixedHeader,variableHeader)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        
class MqttPubRelMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader):
        MqttMsg.__init__(self, fixedHeader,variableHeader)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')

class MqttPubCompMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader):
        MqttMsg.__init__(self, fixedHeader,variableHeader)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')

class MqttPublishMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader, payload):
        MqttMsg.__init__(self, fixedHeader,variableHeader,payload)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        self.topic = variableHeader.get('topic')
        # __payload bytes
        self.payload = payload

class MqttSubscribeMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader, payload=[]):
        MqttMsg.__init__(self, fixedHeader,variableHeader,payload)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        # __payload = [{"topic":"a/b","qos":1}]
        self.payload = payload

class MqttSubAckMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader, payload=[]):
        MqttMsg.__init__(self, fixedHeader,variableHeader,payload)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        # __payload = [0,1,2]
        self.payload = payload

class MqttUnsubscribeMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader, payload=[]):
        MqttMsg.__init__(self, fixedHeader,variableHeader,payload)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')
        # __payload = [topic0,topic1,topic2]
        self.payload = payload
        
class MqttUnsubAckMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader):
        MqttMsg.__init__(self, fixedHeader,variableHeader)
        self.fixedHeader = fixedHeader
        self.messageId = variableHeader.get('messageId')

class MqttMsgFactory:
  
    def message(self, fixedHeader, variableHeader=None, payload=None):
        if fixedHeader.messageType == PINGREQ: 
            return MqttPingReqMsg(fixedHeader)
        elif fixedHeader.messageType == PINGRESP: 
            return MqttPingRespMsg(fixedHeader)
        elif fixedHeader.messageType == DISCONNECT: 
            return MqttDisconnetMsg(fixedHeader)
        elif fixedHeader.messageType == CONNECT:
            return MqttConnectMsg(fixedHeader, variableHeader, payload)
        elif fixedHeader.messageType == CONNACK: 
            return MqttConnAckMsg(fixedHeader, variableHeader)
        elif fixedHeader.messageType == PUBLISH: 
            return MqttPublishMsg(fixedHeader, variableHeader, payload)
        elif fixedHeader.messageType == PUBACK: 
            return MqttPubAckMsg(fixedHeader, variableHeader)
        elif fixedHeader.messageType == PUBREC: 
            return MqttPubRecMsg(fixedHeader, variableHeader)
        elif fixedHeader.messageType == PUBREL: 
            return MqttPubRelMsg(fixedHeader, variableHeader)
        elif fixedHeader.messageType == PUBCOMP: 
            return MqttPubCompMsg(fixedHeader, variableHeader)
        elif fixedHeader.messageType == SUBSCRIBE: 
            return MqttSubscribeMsg(fixedHeader, variableHeader, payload)
        elif fixedHeader.messageType == UNSUBSCRIBE: 
            return MqttUnsubscribeMsg(fixedHeader, variableHeader, payload)
        elif fixedHeader.messageType == SUBACK: 
            return MqttSubAckMsg(fixedHeader, variableHeader, payload)
        elif fixedHeader.messageType == UNSUBACK: 
            return MqttUnsubAckMsg(fixedHeader, variableHeader)
        else:
            return None
        
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
        self.body = []  
        self.state = self.__readingStatusline
        self.__lines = []
        self.__lastHeader = None
        
    def __readStatus(self):
        try:
            statusLine = self.__lines.pop(0)
            [version, status, reason] = statusLine.split(None, 2)
        except ValueError:
            try:
                [version, status] = statusLine.split(None, 1)
                reason = ""
            except ValueError:
                version = ""
        if not version.startswith('HTTP/'):
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
                        length = length + len(datablock)
                        if(isinstance(datablock, list)):
                            datablock = ''.join(datablock)
                        # Only download body to file if status 200
                        if (self.status == 200 and self.f and hasattr(self.f, 'write')):  
                            self.f.write(datablock)
                        else:
                            self.body = self.body + datablock
    #                     datablock = self.__sock.recv()
                        if(len(self.body) < self.length):
                            datablock = self.__sock.recv(1500)
                    self.end()
                else:
                    self.end()
            except Exception,e:
                raise Exception(str(e))
        finally:
            self.end()
    
class HTTPClient:
        
    def __init__(self, host, port, userAgent='InstaMsg'):
        self.version = '1.1'
        self.__userAgent = userAgent
        self.__addr = (host, port)
        self.__sock = None
        self.__checkAddress()
        self.__boundary = '-----------ThIs_Is_tHe_bouNdaRY_78564$!@'
        self.__tcpBufferSize = 1500
        
    def get(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('GET', url, params, headers, body, timeout)
    
    def put(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('PUT', url, params, headers, body, timeout)
    
    def post(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('POST', url, params, headers, body, timeout)  
    
    def delete(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('DELETE', url, params, headers, body, timeout) 
        
    def uploadFile(self, url, filename, params={}, headers={}, timeout=10):
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
        if(not isinstance(filename, str)): raise ValueError('HTTPClient:: download filename should be of type str.')
        f = None
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
            self.__closeFile(f)
            
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
        if(not(isinstance(body, str) or isinstance(body, file) or body is None) ):raise ValueError('HTTPClient:: body should be of type string or file object.')
        try:
            try:
                request = self.__createHttpRequest(method, url, params, headers)
                sizeHint = None
                if(headers.has_key('Content-Length') and isinstance(body, file)):
                    sizeHint = len(request) + headers.get('Content-Length')
    #                 self._sock = Socket(timeout, 0)
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._sock.settimeout(timeout)
    #                 self._sock.connect((self.__addr[0], self.__addr[1]), http=1)
                self._sock.connect((self.__addr[0], self.__addr[1]))
                expect = None
                if(headers.has_key('Expect')):
                    expect = headers['Expect']
                elif(headers.has_key('expect')):
                    expect = headers['expect']
                if(expect and (expect.lower() == '100-continue')):
                    self._sock.sendall(request)
                    httpResponse = HTTPResponse(self._sock, fileObject).response()
                    # Send the remaining body if status 100 received or server that send nothing
                    if(httpResponse.status == 100 or httpResponse.status is None):
                        request = ""
                        self.__send(request, body, fileUploadForm, fileObject, sizeHint)
                        return httpResponse.response()
                    else:
                        raise HTTPResponseError("Expecting status 100, recieved %s" % request.status)
                else:
                    self.__send(request, body, fileUploadForm, fileObject, sizeHint)
                    return HTTPResponse(self._sock, fileObject).response()
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
                self._sock.sendall(request)
        else:
            if(fileUploadForm and len(fileUploadForm) == 2):
                blocksize = 1500    
                if(sizeHint <= self.__tcpBufferSize):
                    if hasattr(body, 'read'): 
                        request = request + ''.join(fileUploadForm[0]) + ''.join(body.read(blocksize)) + ''.join(fileUploadForm[1])
                        self._sock.sendall(request)
                else:
                    request = request + ''.join(fileUploadForm[0])
                    self._sock.sendall(request)
                    partNumber = 1
                    if hasattr(body, 'read'): 
                        partData = body.read(blocksize)
                        while partData:
        #                             self._sock.sendMultiPart(partData, partNumber)
                            self._sock.sendall(partData)
                            partData = body.read(blocksize)
                    if(fileUploadForm and len(fileUploadForm) == 2):
        #                         self._sock.sendMultiPart(fileUploadForm[1], partNumber + 1)
                        self._sock.sendall(fileUploadForm[1])
        #                 self._sock.sendHTTP(self.__addr, request)
    
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
            headerStr = "HTTP/%s\r\nHost: %s\r\n" % (self.version, self.__addr[0])
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

    
    
