# -*- coding: utf-8 -*-
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions

import hashlib
import json
import logging
import time
from threading import Thread, Event

try:
    import ssl

    HAS_SSL = True
except:
    HAS_SSL = False

from .mqtt.client import MqttClient
from .mqtt.client_ws import MqttClientWebSocket
from .mqtt.result import Result
from .mqtt.constants import PROVISIONING_CLIENT_ID
from .mqtt.errors import MqttTimeoutError, MqttConnectError
from .message import Message
from .errors import *
from .constants import *
from .log_handler import ServerLogHandler


# logging.getLogger(__name__).addHandler(logging.NullHandler())

####InstaMsg ###############################################################################

class InstaMsg(Thread):

    def __init__(self, clientId, authKey, connectHandler, disConnectHandler, oneToOneMessageHandler, options={}):
        if (clientId is None): raise ValueError('clientId cannot be null.')
        if (authKey is None or authKey is ''): raise ValueError('authKey cannot be null.')
        if (not callable(connectHandler)): raise ValueError('connectHandler should be a callable object.')
        if (not callable(disConnectHandler)): raise ValueError('disConnectHandler should be a callable object.')
        if (not callable(oneToOneMessageHandler)): raise ValueError(
            'oneToOneMessageHandler should be a callable object.')
        if (clientId):
            if (len(clientId) != 36): raise ValueError(
                'clientId: %s is not a valid uuid e.g. cbf7d550-7204-11e4-a2ad-543530e3bc65'% clientId)
        Thread.__init__(self)
        self.name = 'InstaMsg Thread'
        self.alive = Event()
        self.alive.set()
        self.stopped = Event()
        self.stopped.set()
        self.stopped.clear()
        self.__initLogger()
        self._clientId = clientId
        self._authKey = authKey
        self._onConnectCallBack = connectHandler
        self._onDisConnectCallBack = disConnectHandler
        self._oneToOneMessageHandler = oneToOneMessageHandler
        self._authHash = None
        self._init(clientId, authKey)
        self._logsListener = []
        self._defaultReplyTimeout = INSTAMSG_RESULT_HANDLER_TIMEOUT
        self._msgHandlers = {}
        self._sendMsgReplyHandlers = {}  # {handlerId:{time:122334,handler:replyHandler, timeout:10, timeOutMsg:"Timed out"}}
        self._enableTcp = 1
        self._enableSsl = 1
        self._configHandler = None
        self._rebootHandler = None
        self._updateFirmwareHandler = None
        self._metadata = {}
        self._mqttClient = None
        self._enableLogToServer = 0
        self._initOptions(options)
        clientIdAndUsername = self._getClientIdAndUsername(clientId)
        mqttoptions = self._mqttClientOptions(clientIdAndUsername[1], authKey, self._keepAliveTimer)
        if (self._enableTcp):
            self._mqttClient = MqttClient(INSTAMSG_HOST, self._port, clientIdAndUsername[0], enableSsl=self._enableSsl,
                                          options=mqttoptions)
        else:
            # Try websocket
            self._mqttClient = MqttClientWebSocket(INSTAMSG_HOST, self._port, clientIdAndUsername[0],
                                                   enableSsl=self._enableSsl, options=mqttoptions)
        self._mqttClient.onConnect(self._onConnect)
        self._mqttClient.onDisconnect(self._onDisConnect)
        self._mqttClient.onMessage(self._handleMessage)
        self._connected = 0
        self._mqttClient.connect()

    def _init(self, clientId, authKey):
        if (clientId and authKey):
            self._authHash = hashlib.sha256((clientId + authKey).encode('utf-8')).hexdigest()
            self._filesTopic = "instamsg/clients/%s/files" % clientId
            self._fileUploadUrl = "/api/%s/clients/%s/files" % (INSTAMSG_API_VERSION, clientId)
            self._enableServerLoggingTopic = "instamsg/clients/%s/enableServerLogging" % clientId
            self.serverLogsTopic = "instamsg/clients/%s/logs" % clientId
            self._rebootTopic = "instamsg/clients/%s/reboot" % clientId
            self._infoTopic = "instamsg/clients/%s/info" % clientId
            self._sessionTopic = "instamsg/clients/%s/session" % clientId
            self._metadataTopic = "instamsg/clients/%s/metadata" % clientId
            self._configServerToClientTopic = "instamsg/clients/%s/config/serverToClient" % clientId
            self._configClientToServerTopic = "instamsg/clients/%s/config/clientToServer" % clientId
            self._networkInfoTopic = "instamsg/clients/%s/network" % clientId
            self._updateFirmwareTopic = "instamsg/clients/%s/update_firmware" % clientId

    def _initOptions(self, options):
        if ('configHandler' in options):
            if (not callable(options['configHandler'])): raise ValueError('configHandler should be a callable object.')
            self._configHandler = options['configHandler']
        if ('rebootHandler' in options):
            if (not callable(options['rebootHandler'])): raise ValueError('rebootHandler should be a callable object.')
            self._rebootHandler = options['rebootHandler']
        if( 'updateFirmwareHandler' in options): 
            if(not callable(options['updateFirmwareHandler'])): raise ValueError('updateFirmwareHandler should be a callable object.')
            self._updateFirmwareHandler = options['updateFirmwareHandler']
        if ('enableTcp' in options):
            self._enableTcp = options.get('enableTcp')
        if ('enableLogToServer' in options):
            self._enableLogToServer = options.get('enableLogToServer')
        else:
            self._enableLogToServer = 0
        if ('logLevel' in options):
            self._logLevel = options.get('logLevel')
            if (self._logLevel < INSTAMSG_LOG_LEVEL_DISABLED or self._logLevel > INSTAMSG_LOG_LEVEL_DEBUG):
                raise ValueError("logLevel option should be in between %d and %d" % (
                    INSTAMSG_LOG_LEVEL_DISABLED, INSTAMSG_LOG_LEVEL_DEBUG))
        else:
            self._logLevel = INSTAMSG_LOG_LEVEL_DISABLED
        if ('keepAliveTimer' in options):
            self._keepAliveTimer = options.get('keepAliveTimer')
        else:
            self._keepAliveTimer = INSTAMSG_KEEP_ALIVE_TIMER
        if ('metadata' in options):
            self._metadata = options['metadata']
        self._metadata["instamsg_version"] = INSTAMSG_VERSION
        self._metadata["instamsg_api_version"] = INSTAMSG_API_VERSION
        if 'enableSsl' in options and options.get('enableSsl') and HAS_SSL:
            self._enableSsl = 1
        else:
            self._enableSsl = 0
        self._port = self._getPort(self._enableTcp, self._enableSsl)

    def run(self):
        try:
            while self.alive.isSet():
                try:
                    if self._mqttClient is not None and not self.stopped.isSet():
                        self._mqttClient.process()
                        self._processHandlersTimeout()
                        self._processLogToServerTimeout()
                        time.sleep(0.5)
                except Exception as e:
                    self.logger.error("Error starting InstaMsg (%s) Retrying..." % (str(e)))
                    self.logger.debug("Error starting InstaMsg", exc_info=True)
        finally:
            self.close()

    def close(self):
        try:
            self.stopped.set()
            if (self._mqttClient is not None):
                self._mqttClient.disconnect()
            return 1
        except:
            return -1
        finally:
            self.alive.clear()
            self._mqttClient = None
            self._sendMsgReplyHandlers = None
            self._msgHandlers = None
            self._subscribers = None

    def publish(self, topic, msg, qos=INSTAMSG_QOS0, dup=0, resultHandler=None, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT,
                logging=1):
        if (topic):
            try:
                self._mqttClient.publish(topic, msg, qos, dup, resultHandler, timeout, logging=logging)
            except Exception as e:
                # if logging == 1:
                self.logger.debug("Error while publishing message.", exc_info=True)
                raise InstaMsgPubError(str(e))
        else:
            raise ValueError("Topic cannot be null or empty string.")

    def subscribe(self, topic, qos, msgHandler, resultHandler=None, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        try:
            if (not callable(msgHandler)): raise ValueError('msgHandler should be a callable object.')
            self._msgHandlers[topic] = msgHandler
            if (topic == self._clientId):
                raise ValueError("Canot subscribe to clientId. Instead set oneToOneMessageHandler.")

            def _resultHandler(result):
                if (result.failed()):
                    if (topic in self._msgHandlers):
                        del self._msgHandlers[topic]
                if (callable(resultHandler)): resultHandler(result)

            self._mqttClient.subscribe(topic, qos, _resultHandler, timeout)
        except Exception as e:
            self.logger.debug("Error while subscribing to topic.", exc_info=True)
            raise InstaMsgSubError(str(e))

    def unsubscribe(self, topics, resultHandler, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        try:
            def _resultHandler(result):
                if (result.succeeded()):
                    for topic in topics:
                        if (topic in self._msgHandlers):
                            del self._msgHandlers[topic]
                if (callable(resultHandler)): resultHandler(result)

            self._mqttClient.unsubscribe(topics, _resultHandler, timeout)
        except Exception as e:
            self.logger.debug("Error while unsubscribing from topic.", exc_info=True)
            raise InstaMsgUnSubError(str(e))

    def send(self, clienId, msg, qos=INSTAMSG_QOS0, dup=0, replyHandler=None,
             timeout=INSTAMSG_MSG_REPLY_HANDLER_TIMEOUT):
        try:
            messageId = self._generateMessageId()
            msg = Message(messageId, clienId, msg, qos, dup, replyTopic=self._clientId,
                          instaMsg=self)._sendMsgJsonString()
            self._send(messageId, clienId, msg, qos, dup, replyHandler, timeout)
        except Exception as e:
            self.logger.debug("Error while sending message.", exc_info=True)
            raise InstaMsgSendError(str(e))

    def enableLogToServer(self):
        return self._enableLogToServer

    def connected(self):
        return self._mqttClient.connected()

    @classmethod
    def provision(cls, provId, provPin, enableTcp=1, enableSsl=1, timeout=60):
        try:
            port = cls._getPort(enableTcp, enableSsl)
            if enableTcp:
                mqttClient = MqttClient(INSTAMSG_HOST,
                                        port,
                                        PROVISIONING_CLIENT_ID,
                                        enableSsl=enableSsl)
            else:
                mqttClient = MqttClientWebSocket(INSTAMSG_HOST,
                                                 port,
                                                 PROVISIONING_CLIENT_ID,
                                                 enableSsl=enableSsl)
            return mqttClient.provision(provId, provPin, timeout)
        except MqttTimeoutError as e:
            raise InstaMsgProvisionTimeout(str(e))
        except MqttConnectError as e:
            raise InstaMsgProvisionError(e.value)
        except Exception as e:
            raise InstaMsgProvisionError(str(e))

    def publishConfig(self, config, resultHandler=None):
        try:
            config["instamsg_version"] = INSTAMSG_API_VERSION
            message = json.dumps(config)

            def _resultHandler(result):
                if (result.failed()):
                    if (callable(resultHandler)): resultHandler(Result(config, 0, result.cause()))
                else:
                    if (callable(resultHandler)): resultHandler(Result(config, 1))

            self.publish(self._configClientToServerTopic, message, qos=INSTAMSG_QOS1, dup=0,
                         resultHandler=_resultHandler)
        except Exception as e:
            self.logger.error("Error while publishing config - %s" % str(e))
            self.logger.debug("", exc_info=True)

    def publishNetworkInfo(self, networkInfo):
        networkInfo["instamsg_version"] = INSTAMSG_VERSION
        networkInfo["instamsg_api_version"] = INSTAMSG_API_VERSION
        message = json.dumps(networkInfo)
        self.publish(self._networkInfoTopic, message, INSTAMSG_QOS0, 0)

    def _send(self, messageId, clienId, msg, qos, dup, replyHandler, timeout):
        try:
            if (replyHandler):
                timeOutMsg = "Sending message[%s] %s to %s timed out." % (str(messageId), str(msg), str(clienId))
                self._sendMsgReplyHandlers[messageId] = {'time': time.time(), 'timeout': timeout,
                                                         'handler': replyHandler, 'timeOutMsg': timeOutMsg}

                def _resultHandler(result):
                    if (result.failed()):
                        if (messageId in self._sendMsgReplyHandlers):
                            del self._sendMsgReplyHandlers[messageId]
                    if (callable(replyHandler)): replyHandler(result)
            else:
                _resultHandler = None
            self.publish(clienId, msg, qos, dup, _resultHandler)
        except Exception as e:
            if (messageId in self._sendMsgReplyHandlers):
                del self._sendMsgReplyHandlers[messageId]
            raise Exception(str(e))

    def _generateMessageId(self):
        messageId = self._clientId + "-" + str(int(time.time() * 1000))
        while (messageId in self._sendMsgReplyHandlers):
            messageId = self._clientId + "-" + str(int(time.time() * 1000))
        return messageId

    def _enableServerLogging(self, msg):
        if (msg):
            msgJson = self._parseJson(msg.payload)
            if (msgJson is not None and ('client_id' in msgJson and ('logging' in msgJson))):
                clientId = str(msgJson['client_id'])
                logging = msgJson['logging']
                if (logging == "1"):
                    if (clientId not in self._logsListener):
                        self._logsListener.append(clientId)
                        self._enableLogToServer = 1
                        self._disableServerLoggingTime = time.time() + 1800  # Disable automaticaly in 30 min
                        self.logger.addHandler(self.serverLogHandler)
                else:
                    if (clientId in self._logsListener):
                        self._logsListener.remove(clientId)
                    if (len(self._logsListener) == 0):
                        self._enableLogToServer = 0
                        self.logger.removeHandler(self.serverLogHandler)

    def _processLogToServerTimeout(self):
        if self.enableLogToServer() and self._disableServerLoggingTime < time.time():
            self._enableLogToServer = 0
            self.logger.removeHandler(self.serverLogHandler)

    def _onConnect(self, mqttClient):
        self._connected = 1
        self._sendClientMetadata()
        self.subscribe(self._enableServerLoggingTopic, INSTAMSG_QOS0, self._enableServerLogging)
        time.sleep(10)
        if (self._onConnectCallBack): self._onConnectCallBack(self)

    def _onDisConnect(self):
        if (self._onDisConnectCallBack): self._onDisConnectCallBack()

    def _handleDebugMessage(self, level, msg):
        if (level <= self._logLevel):
            self.log(level, msg)

    def _handleMessage(self, mqttMsg):
        try:
            if (mqttMsg.topic == self._clientId):
                self._handlePointToPointMessage(mqttMsg)
            elif (mqttMsg.topic == self._rebootTopic):
                self._handleSystemRebootMessage()
            elif(mqttMsg.topic == self._updateFirmwareTopic):
                self._handleSystemFirmwareUpdateMessage(mqttMsg)
            elif (mqttMsg.topic == self._configServerToClientTopic):
                self._handleConfigMessage(mqttMsg)
            elif (mqttMsg.topic == self._enableServerLoggingTopic):
                self._enableServerLogging(mqttMsg)
            else:
                self._handlePubSubMessage(mqttMsg)

        except Exception as e:
            self.logger.error("Error while handling message received. - %s" % str(e))
            self.logger.debug("", exc_info=True)

    def _handlePubSubMessage(self, mqttMsg):
        msgHandler = self._msgHandlers.get(mqttMsg.topic)
        if (msgHandler and callable(msgHandler)):
            msg = Message(mqttMsg.messageId, mqttMsg.topic, mqttMsg.payload, mqttMsg.fixedHeader.qos,
                          mqttMsg.fixedHeader.dup)
            msgHandler(msg)

    def _handlePointToPointMessage(self, mqttMsg):
        try:
            msgJson = self._parseJson(mqttMsg.payload)
            messageId, responseId, replyTopic, status = None, None, None, 1
            if ('reply_to' in msgJson):
                replyTopic = msgJson['reply_to']
            else:
                raise ValueError("Send message json should have reply_to address.")
            if ('message_id' in msgJson):
                messageId = msgJson['message_id']
            else:
                raise ValueError("Send message json should have a message_id.")
            if ('response_id' in msgJson):
                responseId = msgJson['response_id']
            if ('body' in msgJson):
                body = msgJson['body']
            if ('status' in msgJson):
                status = int(msgJson['status'])
            qos, dup = mqttMsg.fixedHeader.qos, mqttMsg.fixedHeader.dup
            if (responseId):
                # This is a response to existing message
                if (status == 0):
                    errorCode, errorMsg = None, None
                    if (isinstance(body, dict)):
                        if ("error_code" in body):
                            errorCode = body.get("error_code")
                        if ("error_msg" in body):
                            errorMsg = body.get("error_msg")
                    result = Result(None, 0, (errorCode, errorMsg))
                else:
                    msg = Message(messageId, self._clientId, body, qos, dup, replyTopic=replyTopic, instaMsg=self)
                    result = Result(msg, 1)

                if (responseId in self._sendMsgReplyHandlers):
                    msgHandler = self._sendMsgReplyHandlers.get(responseId).get('handler')
                else:
                    msgHandler = None
                    self.logger.error(
                        "No handler for message [messageId=%s responseId=%s]" % (str(messageId), str(responseId)))
                if (msgHandler):
                    if (callable(msgHandler)): msgHandler(result)
                    del self._sendMsgReplyHandlers[responseId]
            else:
                if (self._oneToOneMessageHandler):
                    msg = Message(messageId, self._clientId, body, qos, dup, replyTopic=replyTopic, instaMsg=self)
                    self._oneToOneMessageHandler(msg)
        except json.JSONDecodeError as e:
            # This could be a normal message published using publish
            # e.g. loopback publish. Handle as normal pub message
            self._handlePubSubMessage(mqttMsg)

    def _mqttClientOptions(self, username, password, keepAliveTimer):
        if (len(password) > INSTAMSG_MAX_BYTES_IN_MSG): raise ValueError(
            "Password length cannot be more than %d bytes." % INSTAMSG_MAX_BYTES_IN_MSG)
        if (keepAliveTimer > 32768 or keepAliveTimer < INSTAMSG_KEEP_ALIVE_TIMER): raise ValueError(
            "keepAliveTimer should be between %d and 32768" % INSTAMSG_KEEP_ALIVE_TIMER)
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
        options['logLevel'] = self._logLevel
        options['reconnectTimer'] = INSTAMSG_RECONNECT_TIMER
        return options

    def _getClientIdAndUsername(self, clientId):
        errMsg = 'clientId is not a valid uuid e.g. cbf7d550-7204-11e4-a2ad-543530e3bc65'
        if (clientId is None): raise ValueError('clientId cannot be null.')
        if (len(clientId) != 36): raise ValueError(errMsg)
        c = clientId.split('-')
        if (len(c) != 5): raise ValueError(errMsg)
        cId = '-'.join(c[0:4])
        userName = c[4]
        if (len(userName) != 12): raise ValueError(errMsg)
        return (cId, userName)

    def _parseJson(self, jsonString):
        return json.loads(jsonString)

    def _processHandlersTimeout(self):
        if self._sendMsgReplyHandlers != {}:
            for key in list(self._sendMsgReplyHandlers):
                value = self._sendMsgReplyHandlers[key]
                if ((time.time() - value['time']) >= value['timeout']):
                    resultHandler = value['handler']
                    if (resultHandler):
                        timeOutMsg = value['timeOutMsg']
                        if (callable(resultHandler)): resultHandler(
                            Result(None, 0, (INSTAMSG_ERROR_TIMEOUT, timeOutMsg)))
                        value['handler'] = None
                    del self._sendMsgReplyHandlers[key]

    def _sendClientMetadata(self):
        try:
            message = json.dumps(self._metadata)
            self.publish(self._infoTopic, message, INSTAMSG_QOS0, 0)
        except Exception as e:
            self.logger.error("Error publishing client metadata (%s). Continuing..." % str(e))
            self.logger.debug("", exc_info=True)

    def _handleConfigMessage(self, mqttMsg):
        msgJson = self._parseJson(mqttMsg.payload)
        if (callable(self._configHandler)):
            self._configHandler(Result((msgJson), 1))

    def _handleSystemRebootMessage(self):
        if (callable(self._rebootHandler)):
            self._rebootHandler()

    def _handleSystemFirmwareUpdateMessage(self, mqttMsg):
        self.logger.info("Firmware update message received.")
        if(callable(self._updateFirmwareHandler)):
            self._updateFirmwareHandler(mqttMsg)

    @classmethod
    def _getPort(cls, enableTcp, enableSsl):
        if enableSsl:
            if HAS_SSL:
                if enableTcp == 1:
                    return INSTAMSG_PORT_SSL
                else:
                    return INSTAMSG_PORT_WS_SSL
            else:
                raise ImportError("SSL not supported, Please check python version and try again.")
        else:
            if enableTcp == 1:
                return INSTAMSG_PORT
            else:
                return INSTAMSG_PORT_WS

    def __initLogger(self):
        self.logger = logging.getLogger("InstaMsg")
        self.serverLogHandler = ServerLogHandler(self)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s:%(lineno)d: %(message)s')
        self.serverLogHandler.setFormatter(formatter)
        self._disableServerLoggingTime = time.time()
