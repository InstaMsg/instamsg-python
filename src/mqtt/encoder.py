# -*- coding: utf-8 -*-

from .constants import *
from .errors import *
from .messages import *

class MqttEncoder:
    def __init__(self):
        pass
    
    def encode(self, mqttMessage):
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
            raise MqttEncoderError('MqttEncoder: Unknown message type - %s' % str(msgType))
    
    def __encodeConnectMsg(self, mqttConnectMessage):
        if(isinstance(mqttConnectMessage, MqttConnectMsg)):
            variableHeaderSize = 12
            fixedHeader = mqttConnectMessage.fixedHeader
            # Encode Payload
            clientId = self.__encodeStringUtf8(mqttConnectMessage.clientId)
            if(not self.__isValidClientId(clientId)):
                raise ValueError("MqttEncoder: invalid clientId: %s should be less than 23 chars in length."% str(clientId))
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
        while 1:
            digit = num % 128
            num= int(num / 128)
            if (num > 0):
                digit |= 0x80
            remainingLength += chr(digit) 
            if(num == 0):
                    break
        return  remainingLength   
    
    def __encodeIntShort(self, number): 
        return chr(int(number / 256)) + chr(number % 256)
    
    def __encodeStringUtf8(self, s):
        return str(s)
    
    def __isValidClientId(self, clientId):   
        if (clientId is None):
            return 0
        length = len(clientId)
        return length >= 1 and length <= 23
