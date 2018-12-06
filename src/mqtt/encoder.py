# -*- coding: utf-8 -*-

import struct
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
            encodedPayload = bytearray()
            encodedPayload.extend(self.__encodeIntShort(len(clientId)))
            encodedPayload.extend(clientId)
            if(mqttConnectMessage.isWillFlag):
                encodedPayload.extend(self.__encodeIntShort(len(mqttConnectMessage.willTopic)))
                encodedPayload.extend(self.__encodeStringUtf8(mqttConnectMessage.willTopic))
                encodedPayload.extend(self.__encodeIntShort(len(mqttConnectMessage.willMessage)))
                encodedPayload.extend(self.__encodeStringUtf8(mqttConnectMessage.willMessage))
            if(mqttConnectMessage.hasUserName):
                encodedPayload.extend(self.__encodeIntShort(len(mqttConnectMessage.username)))
                encodedPayload.extend(self.__encodeStringUtf8(mqttConnectMessage.username))
            if(mqttConnectMessage.hasPassword):
                encodedPayload.extend(self.__encodeIntShort(len(mqttConnectMessage.password)))
                encodedPayload.extend(self.__encodeStringUtf8(mqttConnectMessage.password))
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
            encodedVariableHeader = bytearray()
            encodedVariableHeader.extend(self.__encodeIntShort(len(mqttConnectMessage.protocolName)))
            encodedVariableHeader.extend(self.__encodeStringUtf8(mqttConnectMessage.protocolName))
            encodedVariableHeader.extend(self.__encodeSignedChar(mqttConnectMessage.version))
            encodedVariableHeader.append(connectFlagsByte)                
            encodedVariableHeader.extend(self.__encodeIntShort(mqttConnectMessage.keepAliveTimer))         
            return b''.join([self.__encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload), encodedVariableHeader, encodedPayload])
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttConnectMsg.__name__, mqttConnectMessage.__class__.__name__)) 
            
    def __encodeConnAckMsg(self, mqttConnAckMsg):
        if(isinstance(mqttConnAckMsg, MqttConnAckMsg)):
            fixedHeader = mqttConnAckMsg.fixedHeader
            encodedVariableHeader = mqttConnAckMsg.connectReturnCode
            return b''.join([self.__encodeFixedHeader(fixedHeader, 2, None), bytearray(encodedVariableHeader)])
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttConnAckMsg.__name__, mqttConnAckMsg.__class__.__name__)) 

    def __encodePublishMsg(self, mqttPublishMsg):
        if(isinstance(mqttPublishMsg, MqttPublishMsg)):
            fixedHeader = mqttPublishMsg.fixedHeader
            topic = self.__encodeStringUtf8(mqttPublishMsg.topic)
            variableHeaderSize = 2 + len(topic) 
            if(fixedHeader.qos > 0):
                variableHeaderSize = variableHeaderSize + 2 
            encodedPayload = self.__encodeStringUtf8(mqttPublishMsg.payload)
            # Encode Variable Header
            encodedVariableHeader = bytearray()
            encodedVariableHeader.extend(b''.join([self.__encodeIntShort(len(topic)), topic]))
            if (fixedHeader.qos > 0): 
                encodedVariableHeader.extend(self.__encodeIntShort(mqttPublishMsg.messageId))
            return b''.join([self.__encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload), encodedVariableHeader, encodedPayload])
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPublishMsg.__name__, mqttPublishMsg.__class__.__name__)) 
    
    def __encodeSubscribeMsg(self, mqttSubscribeMsg):
        if(isinstance(mqttSubscribeMsg, MqttSubscribeMsg)):
            fixedHeader = mqttSubscribeMsg.fixedHeader
            variableHeaderSize = 2
            # Encode Payload
            topic = self.__encodeStringUtf8(mqttSubscribeMsg.payload.get('topic'))
            qos = self.__encodeSignedChar(mqttSubscribeMsg.payload.get('qos'))
            encodedPayload = b''.join([self.__encodeIntShort(len(topic)), topic, qos])
            # Encode Variable Header
            encodedVariableHeader = self.__encodeIntShort(mqttSubscribeMsg.messageId)
            return b''.join([self.__encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload), encodedVariableHeader, encodedPayload])
                
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttSubscribeMsg.__name__, mqttSubscribeMsg.__class__.__name__))
    
    def __encodeUnsubscribeMsg(self, mqttUnsubscribeMsg):
        if(isinstance(mqttUnsubscribeMsg, MqttUnsubscribeMsg)):
            fixedHeader = mqttUnsubscribeMsg.fixedHeader
            variableHeaderSize = 2
            # Encode Payload
            encodedPayload = bytearray()
            for topic in mqttUnsubscribeMsg.payload:
                t = self.__encodeStringUtf8(topic)
                encodedPayload.extend(b''.join([self.__encodeIntShort(len(t)), t ]))
            # Encode Variable Header
            encodedVariableHeader = self.__encodeIntShort(mqttUnsubscribeMsg.messageId)
            return b''.join([self.__encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload), encodedVariableHeader, encodedPayload])
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttUnsubscribeMsg.__name__, mqttUnsubscribeMsg.__class__.__name__))
    
    def __encodeSubAckMsg(self, mqttSubAckMsg):
        if(isinstance(mqttSubAckMsg, MqttSubAckMsg)):
            fixedHeader = mqttSubAckMsg.fixedHeader
            variableHeaderSize = 2
            # Encode Payload
            encodedPayload = bytearray()
            for qos in mqttSubAckMsg.payload:
                encodedPayload.extend(self.__encodeSignedChar(qos))
            # Encode Variable Header
            encodedVariableHeader = self.__encodeIntShort(mqttSubAckMsg.messageId)
            return b''.join([self.__encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload), encodedVariableHeader, encodedPayload])
            
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttSubAckMsg.__name__, mqttSubAckMsg.__class__.__name__))
    
    def __encodeFixedHeaderAndMessageIdOnlyMsg(self, mqttMessage):
        msgType = mqttMessage.fixedHeader.messageType
        if(isinstance(mqttMessage, MqttUnsubscribeMsg) \
            or isinstance(mqttMessage, MqttPubAckMsg)  \
            or isinstance(mqttMessage, MqttPubRecMsg)  \
            or isinstance(mqttMessage, MqttPubCompMsg) \
            or isinstance(mqttMessage, MqttPubRelMsg)):
            fixedHeader = mqttMessage.fixedHeader
            variableHeaderSize = 2
            # Encode Variable Header
            encodedVariableHeader = self.__encodeIntShort(mqttMessage.messageId)
            return b''.join([self.__encodeFixedHeader(fixedHeader, variableHeaderSize, None), encodedVariableHeader])
        else:
            if msgType == UNSUBACK: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttUnsubAckMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == PUBACK: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubAckMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == PUBREC: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubRecMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == PUBCOMP: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubCompMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == PUBREL: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubRelMsg.__name__, mqttMessage.__class__.__name__))
    
    def __encodeFixedHeaderOnlyMsg(self, mqttMessage):
        msgType = mqttMessage.fixedHeader.messageType
        if(isinstance(mqttMessage, MqttPingReqMsg)      \
            or isinstance(mqttMessage, MqttPingRespMsg) \
            or isinstance(mqttMessage, MqttDisconnetMsg)):
            return self.__encodeFixedHeader(mqttMessage.fixedHeader, 0, None)
        else:
            if msgType == PINGREQ: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPingReqMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == PINGRESP: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPingRespMsg.__name__, mqttMessage.__class__.__name__))
            if msgType == DISCONNECT: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttDisconnetMsg.__name__, mqttMessage.__class__.__name__))
    
    def __encodeFixedHeader(self, fixedHeader, variableHeaderSize, encodedPayload):
        encodedFixedHeader = bytearray()
        if encodedPayload is None:
            length = 0
        else: length = len(encodedPayload)
        encodedRemainingLength = self.__encodeRemainingLength(variableHeaderSize + length)
        encodedFixedHeader.append(self.__getFixedHeaderFirstByte(fixedHeader))
        encodedFixedHeader.extend(encodedRemainingLength)
        return encodedFixedHeader
    
    def __getFixedHeaderFirstByte(self, fixedHeader):
        firstByte = fixedHeader.messageType
        if (fixedHeader.dup):
            firstByte |= 0x08;
        firstByte |= fixedHeader.qos << 1;
        if (fixedHeader.retain):
            firstByte |= 0x01;
        return firstByte;
    
    def __encodeRemainingLength(self, num):
        remainingLengthBytes = bytearray()
        while 1:
            digit = num % 128
            num= num // 128
            if (num > 0):
                digit |= 0x80
            remainingLengthBytes.append(digit)
            if(num == 0):
                    break
        return  remainingLengthBytes   
    
    def __encodeSignedChar(self, number): 
        return struct.pack("!b", number)

    def __encodeIntShort(self, number): 
        return struct.pack("!H", number)
    
    def __encodeStringUtf8(self, s):
        return s.encode('utf-8')
        
    def __isValidClientId(self, clientId):   
        if (clientId is None):
            return 0
        length = len(clientId)
        return length >= 1 and length <= 23
