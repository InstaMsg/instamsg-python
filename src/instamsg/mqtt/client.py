# -*- coding: utf-8 -*-
import json
import logging
import socket
import time
import traceback
from threading import RLock

try:
    import ssl
except ImportError:
    ssl = None

from .messages import MqttFixedHeader, MqttMsgFactory
from .decoder import MqttDecoder
from .encoder import MqttEncoder
from .result import Result
from .errors import *
from .constants import *


class MqttClient:

    def __init__(self, host, port, clientId, enableSsl=0, options={}):
        if (not clientId):
            raise ValueError('MqttClient:: clientId cannot be null.')
        if (len(clientId) > MQTT_MAX_CLIENT_ID_LENGTH):
            raise ValueError(
                'MqttClient:: clientId cannot length cannot be greater than %s.' % MQTT_MAX_CLIENT_ID_LENGTH)
        if (not host):
            raise ValueError('MqttClient:: host cannot be null.')
        if (not port):
            raise ValueError('MqttClient:: port cannot be null.')
        if ssl is None and enableSsl == 1:
            raise ValueError('MqttClient:: cannot enable ssl as ssl library missing')
        self.logger = logging.getLogger("InstaMsg")
        self.errCount = 0
        self.lock = RLock()
        self.host = host
        self.port = port
        self.clientId = clientId
        self.enableSsl = enableSsl
        self.options = self._initOptions(options)
        self.keepAliveTimer = self.options['keepAliveTimer']
        self.reconnectTimer = self.options['reconnectTimer']
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
        self._msgIdInbox = []
        self._resultHandlers = {}  # {handlerId:{time:122334,handler:replyHandler, timeout:10, timeOutMsg:"Timed out"}}
        self._lastConnectTime = time.time()
        self.decoderErrorCount = 0
        self._connectAckTimeoutCount = 0
        self.internet_address_family = socket.AF_INET
        self.keepAliveTimeout = KEEP_ALIVE_TIMEOUT

    def _initOptions(self, options):
        options['clientId'] = self.clientId
        if (not 'hasUserName' in options): options['hasUserName'] = 0
        if (not 'username' in options): options['username'] = ''
        if (not 'hasPassword' in options): options['hasPassword'] = 0
        if (not 'password' in options): options['password'] = ''
        if (not 'keepAliveTimer' in options): options['keepAliveTimer'] = 60
        if (options['keepAliveTimer'] > MQTT_MAX_KEEP_ALIVE_TIMER): raise ValueError(
            "keepAliveTimer should be less than 64800")
        if (not 'hasPassword' in options): options['hasPassword'] = 0
        if (not 'isCleanSession' in options): options['isCleanSession'] = 1
        if (not 'isWillFlag' in options): options['isWillFlag'] = 0
        if (not 'willQos' in options): options['willQos'] = 0
        if (not 'isWillRetain' in options): options['isWillRetain'] = 0
        if (not 'willTopic' in options): options['willTopic'] = ""
        if (not 'willMessage' in options): options['willMessage'] = ""
        if (not 'logLevel' in options): options['logLevel'] = MQTT_LOG_LEVEL_DEBUG
        if (not 'reconnectTimer' in options): options['reconnectTimer'] = MQTT_SOCKET_RECONNECT_TIMER
        return options

    def process(self):
        try:
            if (not self._disconnecting):
                self.connect()
                if (self._sockInit):
                    mqttMsg = self._receive()
                    if (mqttMsg): self._handleMqttMessage(mqttMsg)
                    if ((self._lastPingReqTime + self.keepAliveTimeout) < time.time() and self._lastPingRespTime is None):
                            self.disconnect()
                    if ((self._lastPingReqTime + self.keepAliveTimer) < time.time()):
                        self._sendPingReq()
                        self._lastPingReqTime = time.time()
                        self._lastPingRespTime = None
                self._processHandlersTimeout()
                if (self._connecting and ((self._lastConnectTime + CONNECT_ACK_TIMEOUT) < time.time())):
                    self.logger.info("Connect Ack timed out. Resetting connection.")
                    self._resetSock()
        except (socket.error, socket.herror, socket.gaierror) as msg :
            if msg.errno == 101:
                self._toggle_socket_family()
            self._resetSock()
            self.logger.error("Socket error (%s)" % (str(msg)))
            self.logger.debug("", exc_info=True)
        except MqttConnectError as msg:
            self.logger.error("MqttConnectError (%s)" % (str(msg)))
            self.logger.debug("", exc_info=True)
        except Exception as msg:
            self.logger.error("Unknown error (%s)" % (str(msg)))
            self.logger.debug("", exc_info=True)

    def provision(self, provId, provPin, timeout=300):
        try:
            auth = None
            self._initSock()
            if (self._sockInit):
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
                self._sendallDataToSocket(encodedMsg)
                timeout = time.time() + timeout
                while (timeout > time.time()):
                    time.sleep(10)
                    mqttMsg = self._receive()
                    if (mqttMsg and mqttMsg.fixedHeader.messageType == PROVACK):
                        auth = self._getAuthInfoFromProvAckMsg(mqttMsg)
                        return auth
                raise(MqttTimeoutError("Provisioning timed out."))
        finally:
            self._closeSocket()

    def connect(self):
        try:
            self._initSock()
            if (self._connecting is 0 and self._sockInit):
                if (not self._connected):
                    self._connecting = 1
                    fixedHeader = MqttFixedHeader(CONNECT, qos=0, dup=0, retain=0)
                    connectMsg = self._mqttMsgFactory.message(fixedHeader, self.options, self.options)
                    encodedMsg = self._mqttEncoder.encode(connectMsg)
                    self._sendallDataToSocket(encodedMsg)
        except socket.timeout:
            self._connecting = 0
            self.logger.error("Socket timed out sending connect.")
            self.logger.debug("", exc_info=True)
        except (socket.error, socket.herror, socket.gaierror) as msg :
            if msg.errno == 101:
                self._toggle_socket_family()
            self._resetSock()
            self.logger.error("Socket Error -  %s" % (str(msg)))
            self.logger.debug("", exc_info=True)
        except Exception as e:
            self._connecting = 0
            self.logger.error("Unknown error in connect  %s" % (str(e)))
            self.logger.debug("", exc_info=True)

    def disconnect(self):
        try:
            try:
                self._disconnecting = 1
                if (not self._connecting and not self._waitingReconnect and self._sockInit):
                    fixedHeader = MqttFixedHeader(DISCONNECT, qos=0, dup=0, retain=0)
                    disConnectMsg = self._mqttMsgFactory.message(fixedHeader)
                    encodedMsg = self._mqttEncoder.encode(disConnectMsg)
                    self._sendallDataToSocket(encodedMsg)
            except Exception as e:
                self.logger.error("Unknown error in socket connect  %s" % (str(e)))
                self.logger.debug("", exc_info=True)
        finally:
            self._resetSock()

    def publish(self, topic, payload, qos=MQTT_QOS0, dup=0, resultHandler=None,
                resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT, retain=0, logging=1):
        if (not self._connected or self._connecting or self._waitingReconnect):
            raise MqttClientError("Cannot publish message as not connected.")
        self._validateTopic(topic)
        self._validateQos(qos)
        self._validateResultHandler(resultHandler)
        self._validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(PUBLISH, qos, dup=0, retain=0)
        messageId = 0
        if (qos > MQTT_QOS0): messageId = self._generateMessageId()
        variableHeader = {'messageId': messageId, 'topic': str(topic)}
        publishMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader, payload)
        encodedMsg = self._mqttEncoder.encode(publishMsg)
        if (logging):
            self.logger.debug('Publishing message: %s' % publishMsg.toString())
        if (qos > MQTT_QOS0 and messageId and resultHandler):
            timeOutMsg = 'Publishing message %s to topic %s with qos %d timed out.' % (payload, topic, qos)
            self._resultHandlers[messageId] = {'time': time.time(), 'timeout': resultHandlerTimeout,
                                               'handler': resultHandler, 'timeOutMsg': timeOutMsg}
        self._sendallDataToSocket(encodedMsg)
        if (qos == MQTT_QOS0 and resultHandler and callable(resultHandler)):
            resultHandler(Result(None, 1))  # immediately return messageId 0 in case of qos 0

    def subscribe(self, topic, qos, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT):
        if (not self._connected or self._connecting or self._waitingReconnect):
            raise MqttClientError("Cannot subscribe as not connected.")
        self._validateTopic(topic)
        self._validateQos(qos)
        self._validateResultHandler(resultHandler)
        self._validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(SUBSCRIBE, qos=1, dup=0, retain=0)
        messageId = self._generateMessageId()
        variableHeader = {'messageId': messageId}
        subMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader, {'topic': topic, 'qos': qos})
        encodedMsg = self._mqttEncoder.encode(subMsg)
        self.logger.debug('Sending subscribe message: %s' % subMsg.toString())
        if (resultHandler):
            timeOutMsg = 'Subscribe to topic %s with qos %d timed out.' % (topic, qos)
            self._resultHandlers[messageId] = {'time': time.time(), 'timeout': resultHandlerTimeout,
                                               'handler': resultHandler, 'timeOutMsg': timeOutMsg}
        self._sendallDataToSocket(encodedMsg)

    def unsubscribe(self, topics, resultHandler=None, resultHandlerTimeout=MQTT_RESULT_HANDLER_TIMEOUT):
        if (not self._connected or self._connecting or self._waitingReconnect):
            raise MqttClientError("Cannot unsubscribe as not connected.")
        self._validateResultHandler(resultHandler)
        self._validateTimeout(resultHandlerTimeout)
        fixedHeader = MqttFixedHeader(UNSUBSCRIBE, qos=1, dup=0, retain=0)
        messageId = self._generateMessageId()
        variableHeader = {'messageId': messageId}
        if (isinstance(topics, str)):
            topics = [topics]
        if (isinstance(topics, list)):
            for topic in topics:
                self._validateTopic(topic)
                unsubMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader, topics)
                encodedMsg = self._mqttEncoder.encode(unsubMsg)
                self.logger.debug('Sending unsubscribe message: %s' % unsubMsg.toString())
                if (resultHandler):
                    timeOutMsg = 'Unsubscribe to topics %s timed out.' % str(topics)
                    self._resultHandlers[messageId] = {'time': time.time(), 'timeout': resultHandlerTimeout,
                                                       'handler': resultHandler, 'timeOutMsg': timeOutMsg}
                self._sendallDataToSocket(encodedMsg)
                return messageId
        else:
            raise TypeError('Topics should be an instance of string or list.')

    def onConnect(self, callback):
        if (callable(callback)):
            self._onConnectCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.')

    def connected(self):
        return self._connected and not self._disconnecting

    def onDisconnect(self, callback):
        if (callable(callback)):
            self._onDisconnectCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.')

    def onMessage(self, callback):
        if (callable(callback)):
            self._onMessageCallBack = callback
        else:
            raise ValueError('Callback should be a callable object.')

    def _toggle_socket_family(self):
        ## In case the interface doesnot support IPV4 by default try IPV6
        ## Keep toggling between then and trying
        if self.internet_address_family == socket.AF_INET:
            self.internet_address_family = socket.AF_INET6
        else:
            self.internet_address_family = socket.AF_INET

    def _setSocketNConnect(self):
        self.logger.info('Connecting to %s:%s' % (self.host, str(self.port)))
        self._sock = socket.socket(self.internet_address_family, socket.SOCK_STREAM, 0)
        self._sock.settimeout(MQTT_SOCKET_TIMEOUT)
        if self.enableSsl and ssl:
            self._sock = ssl.wrap_socket(self._sock,
                                         cert_reqs=ssl.CERT_NONE,
                                         ssl_version=ssl.PROTOCOL_SSLv23)
        self._sock.connect((self.host, self.port))
        self.logger.debug('Socket opened to %s:%s' % (self.host, str(self.port)))

    def _getDataFromSocket(self):
        try:
            if self._sock is not None:
                self.lock.acquire()
                try:
                    # Do not put any logs in this block. Will cause thread deadlock while
                    # logging via ServerLogHandler
                    return self._sock.recv(MQTT_SOCKET_MAX_BYTES_READ)
                finally:
                    self.lock.release()
        except socket.timeout:
            pass
        except socket.error as err:
            if ssl and isinstance(err, (ssl.SSLWantWriteError, ssl.SSLWantReadError)):
                pass
            else:
                self._resetSock()
                self.logger.error("SocketError" % (str(err)))
                self.logger.debug("", exc_info=True)


    def _sendallDataToSocket(self, data):
        try:
            if self._sock is not None:
                self.lock.acquire()
                try:
                    # Do not put any logs in this block. Will cause thread deadlock while
                    # logging via ServerLogHandler
                    self._sock.sendall(data)
                finally:
                    self.lock.release()
        except socket.error as err:
            if ssl and isinstance(err, (ssl.SSLWantWriteError, ssl.SSLWantReadError)):
                pass
            else:
                self._resetSock()
                raise socket.error(str("Socket error in send: %s. Connection reset." % (str(err))))


    def _validateTopic(self, topic):
        if (topic):
            pass
        else:
            raise ValueError('Topics cannot be Null or empty.')
        if (len(topic) < MQTT_MAX_TOPIC_LEN + 1):
            pass
        else:
            raise ValueError('Topic length cannot be more than %d' % MQTT_MAX_TOPIC_LEN)

    def _validateQos(self, qos):
        if (not isinstance(qos, int) or qos < MQTT_QOS0 or qos > MQTT_QOS2):
            raise ValueError('Qos should be a between %d and %d.' % (MQTT_QOS0, self.MQTT_QOS2))

    def _validateRetain(self, retain):
        if (not isinstance(retain, int) or retain != 0 or retain != 1):
            raise ValueError('Retain can only be integer 0 or 1')

    def _validateTimeout(self, timeout):
        if (not isinstance(timeout, int) or timeout < 0 or timeout > MQTT_MAX_RESULT_HANDLER_TIMEOUT):
            raise ValueError('Timeout can only be integer between 0 and %d.' % MQTT_MAX_RESULT_HANDLER_TIMEOUT)

    def _validateResultHandler(self, resultHandler):
        if (resultHandler is not None and not callable(resultHandler)):
            raise ValueError('Result Handler should be a callable object.')


    def _receive(self):
        try:
            data = self._getDataFromSocket()
            if (data is not None) and (len(data) > 0):
                mqttMsg = self._mqttDecoder.decode(data)
                self.decoderErrorCount = 0
                if (mqttMsg):
                    self.logger.debug('Received message:%s' % mqttMsg.toString())
            else:
                mqttMsg = None
            return mqttMsg
        except MqttDecoderError as msg:
            self.decoderErrorCount = self.decoderErrorCount + 1
            self.logger.debug("MqttDecoderError.", exc_info=True)
            if (self.decoderErrorCount > MQTT_DECODER_ERROR_COUNT_FOR_SOCKET_RESET):
                self.logger.debug("Resetting socket as MqttDecoderError count exceeded %s" %
                                  str(MQTT_DECODER_ERROR_COUNT_FOR_SOCKET_RESET), exc_info=True)
                self.decoderErrorCount = 0
                self._resetSock()
        except MqttFrameError as msg:
            self._resetSock()
            self.logger.debug("MqttFrameError. Resetting socket", exc_info=True)

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
        if (resultHandlerDict):
            resultHandler = resultHandlerDict.get('handler')
        return resultHandler

    def _handleSubAck(self, mqttMessage):
        resultHandler = self._getResultHandler(mqttMessage)
        if (resultHandler):
            if (callable(resultHandler)): resultHandler(Result(mqttMessage, 1))
            del self._resultHandlers[mqttMessage.messageId]

    def _handleUnSubAck(self, mqttMessage):
        resultHandler = self._getResultHandler(mqttMessage)
        if (resultHandler):
            if (callable(resultHandler)): resultHandler(Result(mqttMessage, 1))
            del self._resultHandlers[mqttMessage.messageId]

    def _onPublish(self, mqttMessage):
        resultHandler = self._getResultHandler(mqttMessage)
        if (resultHandler):
            if (callable(resultHandler)): resultHandler(Result(mqttMessage, 1))
            del self._resultHandlers[mqttMessage.messageId]

    def _handleConnAckMsg(self, mqttMessage):
        self._connecting = 0
        connectReturnCode = mqttMessage.connectReturnCode
        if (connectReturnCode == CONNECTION_ACCEPTED):
            self._connected = 1
            self._lastConnectTime = time.time()
            self.logger.info('Connected successfully to %s:%s' % (self.host, str(self.port)))
            if (self._onConnectCallBack): self._onConnectCallBack(self)
        else:
            self._handleProvisionAndConnectAckCode("Connection", connectReturnCode)

    def _getAuthInfoFromProvAckMsg(self, mqttMessage):
        provisionReturnCode = mqttMessage.provisionReturnCode
        payload = mqttMessage.payload
        provisioningData = {}
        if not payload and provisionReturnCode in [PROVISIONING_SUCCESSFUL, PROVISIONING_SUCCESSFUL_WITH_CERT]:
            raise MqttConnectError((0, "Empty payload received from server."))
        if provisionReturnCode == PROVISIONING_SUCCESSFUL:
            provisioningData['client_id'] = payload[0:36]
            provisioningData['auth_token'] = payload[37:]
            provisioningData['secure_ssl_certificate'] = 0
            provisioningData['key'] = ''
            provisioningData['certificate'] = ''
            return provisioningData
        elif provisionReturnCode == PROVISIONING_SUCCESSFUL_WITH_CERT:
            payloadJson = json.loads(payload)
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
        msg = (-1,'')
        if (code == CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION):
            msg = (1, '[MqttClient]:: %s refused unacceptable mqtt protocol version.' % type)
        elif (code == CONNECTION_REFUSED_IDENTIFIER_REJECTED):
            msg = (2, '[MqttClient]:: %s refused client identifier rejected.' % type)
        elif (code == CONNECTION_REFUSED_SERVER_UNAVAILABLE):
            msg = (3, '[MqttClient]:: %s refused server unavailable. Waiting ...' % type)
            # self.logger.debug(msg)
            # return  # Not an error just wait for server
        elif (code == CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD):
            msg = (4, '[MqttClient]:: %s refused bad username or password.' % type)
        elif (code == CONNECTION_REFUSED_NOT_AUTHORIZED):
            msg = (5, '[MqttClient]:: %s refused not authorized.' % type)
        raise MqttConnectError(msg)  # Error should be bubbled up

    def _handlePublishMsg(self, mqttMessage):
        if (mqttMessage.fixedHeader.qos > MQTT_QOS1):
            if (mqttMessage.messageId not in self._msgIdInbox):
                self._msgIdInbox.append(mqttMessage.messageId)
        if (self._onMessageCallBack):
            self._onMessageCallBack(mqttMessage)
        if (mqttMessage.fixedHeader.qos == MQTT_QOS1):
            self._sendPubAckMsg(mqttMessage)

    def _sendPubAckMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(PUBACK)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubAckMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self._mqttEncoder.encode(pubAckMsg)
        self._sendallDataToSocket(encodedMsg)

    def _handlePubAckMsg(self, mqttMessage):
        resultHandler = self._getResultHandler(mqttMessage)
        if (resultHandler):
            if (callable(resultHandler)): resultHandler(Result(mqttMessage.messageId, 1, None))
            resultHandler = None
            del self._resultHandlers[mqttMessage.messageId]

    def _handlePubRelMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(PUBCOMP)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubComMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self._mqttEncoder.encode(pubComMsg)
        self._sendallDataToSocket(encodedMsg)
        if (mqttMessage.messageId in self._msgIdInbox):
            self._msgIdInbox.remove(mqttMessage.messageId)

    def _handlePubRecMsg(self, mqttMessage):
        fixedHeader = MqttFixedHeader(PUBREL, 1)
        variableHeader = {'messageId': mqttMessage.messageId}
        pubRelMsg = self._mqttMsgFactory.message(fixedHeader, variableHeader)
        encodedMsg = self._mqttEncoder.encode(pubRelMsg)
        self._sendallDataToSocket(encodedMsg)

    def _resetSock(self):
        self._disconnecting = 1
        self.logger.info('Resetting connection due to socket error or connect timeout...')
        self._closeSocket()

    def _initSock(self):
        t = time.time()
        #         if (self._sockInit is 0 and self._nextConnTry - t > 0): raise SocketError('Last connection failed. Waiting before retry.')
        waitFor = self._nextConnTry - t
        if (self._sockInit is 0 and waitFor > 0):
            if (not self._waitingReconnect):
                self._waitingReconnect = 1
                self.logger.info('Last connection failed. Waiting  for %d seconds before retry...' % int(
                    waitFor))
            return
        if (self._sockInit is 0 and waitFor <= 0):
            self._nextConnTry = t + self.reconnectTimer
            if (self._sock is not None):
                self._closeSocket()
            self._setSocketNConnect()
            self._sockInit = 1
            self._waitingReconnect = 0

    def _closeSocket(self):
        try:
            self.logger.info('Closing socket...')
            if (self._sock):
                self._sock.close()
                self._sock = None
        except Exception as e:
            self.logger.error("Error closing socket (%s)." % (str(e)))
            self.logger.debug("", exc_info=True)
        finally:
            self._sockInit = 0
            self._connected = 0
            self._connecting = 0
            self._disconnecting = 0
            self._lastPingReqTime = time.time()
            self._lastPingRespTime = self._lastPingReqTime
            if (self._onDisconnectCallBack): self._onDisconnectCallBack()

    def _generateMessageId(self):
        if self._messageId == MQTT_MAX_INT:
            self._messageId = 0
        self._messageId = self._messageId + 1
        return self._messageId

    def _processHandlersTimeout(self):
        for key in list(self._resultHandlers):
            value = self._resultHandlers[key]
            if ((time.time() - value['time']) >= value['timeout']):
                resultHandler = value['handler']
                if (resultHandler and callable(resultHandler)):
                    timeOutMsg = value['timeOutMsg']
                    resultHandler(Result(None, 0, (MQTT_ERROR_TIMEOUT, timeOutMsg)))
                    value['handler'] = None
                del self._resultHandlers[key]

    def _sendPingReq(self):
        fixedHeader = MqttFixedHeader(PINGREQ)
        pingReqMsg = self._mqttMsgFactory.message(fixedHeader)
        encodedMsg = self._mqttEncoder.encode(pingReqMsg)
        self._sendallDataToSocket(encodedMsg)