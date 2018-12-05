# -*- coding: utf-8 -*-

from .constants import *
        
class MqttFixedHeader:
    def __init__(self, messageType=None, qos=MQTT_QOS0, dup=0, retain=0, remainingLength=0):
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
        self.protocolName = MQTT_PROTOCOL_NAME
        self.version = MQTT_PROTOCOL_VERSION
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
        
class MqttProvAckMsg(MqttMsg):
    def __init__(self, fixedHeader, variableHeader, payload):
        MqttMsg.__init__(self, fixedHeader, variableHeader)
        self.fixedHeader = fixedHeader
        self.__variableHeader = variableHeader
        self.provisionReturnCode = variableHeader.get('provisionReturnCode')
        self.payload = payload
        
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
        self.payload = payload.strip()

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
        elif fixedHeader.messageType == PROVACK: 
            return MqttProvAckMsg(fixedHeader, variableHeader, payload)
        else:
            return None