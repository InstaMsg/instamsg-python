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
            return self._encodeConnectMsg(mqttMessage) 
        elif msgType == CONNACK:
            return self._encodeConnAckMsg(mqttMessage)
        elif msgType == PUBLISH:
            return self._encodePublishMsg(mqttMessage)
        elif msgType == SUBSCRIBE:
            return self._encodeSubscribeMsg(mqttMessage)
        elif msgType == UNSUBSCRIBE:
            return self._encodeUnsubscribeMsg(mqttMessage)
        elif msgType == SUBACK:
            return self._encodeSubAckMsg(mqttMessage)
        elif msgType in [UNSUBACK, PUBACK, PUBREC, PUBCOMP, PUBREL]:
            return self._encodeFixedHeaderAndMessageIdOnlyMsg(mqttMessage)
        elif msgType in [PINGREQ, PINGRESP, DISCONNECT]:
            return self._encodeFixedHeaderOnlyMsg(mqttMessage)
        else:
            raise MqttEncoderError('MqttEncoder: Unknown message type - %s' % str(msgType))
    
    def _encodeConnectMsg(self, mqttConnectMessage):
        if(isinstance(mqttConnectMessage, MqttConnectMsg)):
            variableHeaderSize = 12
            fixedHeader = mqttConnectMessage.fixedHeader
            # Encode Payload
            clientId = self._encodeStringUtf8(mqttConnectMessage.clientId)
            if(not self._isValidClientId(clientId)):
                raise ValueError("MqttEncoder: invalid clientId: %s should be less than 23 chars in length."% str(clientId))
            encodedPayload = bytearray()
            encodedPayload.extend(self._encodeIntShort(len(clientId)))
            encodedPayload.extend(clientId)
            if(mqttConnectMessage.isWillFlag):
                encodedPayload.extend(self._encodeIntShort(len(mqttConnectMessage.willTopic)))
                encodedPayload.extend(self._encodeStringUtf8(mqttConnectMessage.willTopic))
                encodedPayload.extend(self._encodeIntShort(len(mqttConnectMessage.willMessage)))
                encodedPayload.extend(self._encodeStringUtf8(mqttConnectMessage.willMessage))
            if(mqttConnectMessage.hasUserName):
                encodedPayload.extend(self._encodeIntShort(len(mqttConnectMessage.username)))
                encodedPayload.extend(self._encodeStringUtf8(mqttConnectMessage.username))
            if(mqttConnectMessage.hasPassword):
                encodedPayload.extend(self._encodeIntShort(len(mqttConnectMessage.password)))
                encodedPayload.extend(self._encodeStringUtf8(mqttConnectMessage.password))
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
            encodedVariableHeader.extend(self._encodeIntShort(len(mqttConnectMessage.protocolName)))
            encodedVariableHeader.extend(self._encodeStringUtf8(mqttConnectMessage.protocolName))
            encodedVariableHeader.extend(self._encodeSignedChar(mqttConnectMessage.version))
            encodedVariableHeader.append(connectFlagsByte)                
            encodedVariableHeader.extend(self._encodeIntShort(mqttConnectMessage.keepAliveTimer))         
            return b''.join([self._encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload), encodedVariableHeader, encodedPayload])
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttConnectMsg._name_, mqttConnectMessage._class_._name_)) 
            
    def _encodeConnAckMsg(self, mqttConnAckMsg):
        if(isinstance(mqttConnAckMsg, MqttConnAckMsg)):
            fixedHeader = mqttConnAckMsg.fixedHeader
            encodedVariableHeader = mqttConnAckMsg.connectReturnCode
            return b''.join([self._encodeFixedHeader(fixedHeader, 2, None), bytearray(encodedVariableHeader)])
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttConnAckMsg._name_, mqttConnAckMsg._class_._name_)) 

    def _encodePublishMsg(self, mqttPublishMsg):
        if(isinstance(mqttPublishMsg, MqttPublishMsg)):
            fixedHeader = mqttPublishMsg.fixedHeader
            topic = self._encodeStringUtf8(mqttPublishMsg.topic)
            variableHeaderSize = 2 + len(topic) 
            if(fixedHeader.qos > 0):
                variableHeaderSize = variableHeaderSize + 2 
            encodedPayload = self._encodeStringUtf8(mqttPublishMsg.payload)
            # Encode Variable Header
            encodedVariableHeader = bytearray()
            encodedVariableHeader.extend(b''.join([self._encodeIntShort(len(topic)), topic]))
            if (fixedHeader.qos > 0): 
                encodedVariableHeader.extend(self._encodeIntShort(mqttPublishMsg.messageId))
            return b''.join([self._encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload), encodedVariableHeader, encodedPayload])
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPublishMsg._name_, mqttPublishMsg._class_._name_)) 
    
    def _encodeSubscribeMsg(self, mqttSubscribeMsg):
        if(isinstance(mqttSubscribeMsg, MqttSubscribeMsg)):
            fixedHeader = mqttSubscribeMsg.fixedHeader
            variableHeaderSize = 2
            # Encode Payload
            topic = self._encodeStringUtf8(mqttSubscribeMsg.payload.get('topic'))
            qos = self._encodeSignedChar(mqttSubscribeMsg.payload.get('qos'))
            encodedPayload = b''.join([self._encodeIntShort(len(topic)), topic, qos])
            # Encode Variable Header
            encodedVariableHeader = self._encodeIntShort(mqttSubscribeMsg.messageId)
            return b''.join([self._encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload), encodedVariableHeader, encodedPayload])
                
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttSubscribeMsg._name_, mqttSubscribeMsg._class_._name_))
    
    def _encodeUnsubscribeMsg(self, mqttUnsubscribeMsg):
        if(isinstance(mqttUnsubscribeMsg, MqttUnsubscribeMsg)):
            fixedHeader = mqttUnsubscribeMsg.fixedHeader
            variableHeaderSize = 2
            # Encode Payload
            encodedPayload = bytearray()
            for topic in mqttUnsubscribeMsg.payload:
                t = self._encodeStringUtf8(topic)
                encodedPayload.extend(b''.join([self._encodeIntShort(len(t)), t ]))
            # Encode Variable Header
            encodedVariableHeader = self._encodeIntShort(mqttUnsubscribeMsg.messageId)
            return b''.join([self._encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload), encodedVariableHeader, encodedPayload])
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttUnsubscribeMsg._name_, mqttUnsubscribeMsg._class_._name_))
    
    def _encodeSubAckMsg(self, mqttSubAckMsg):
        if(isinstance(mqttSubAckMsg, MqttSubAckMsg)):
            fixedHeader = mqttSubAckMsg.fixedHeader
            variableHeaderSize = 2
            # Encode Payload
            encodedPayload = bytearray()
            for qos in mqttSubAckMsg.payload:
                encodedPayload.extend(self._encodeSignedChar(qos))
            # Encode Variable Header
            encodedVariableHeader = self._encodeIntShort(mqttSubAckMsg.messageId)
            return b''.join([self._encodeFixedHeader(fixedHeader, variableHeaderSize, encodedPayload), encodedVariableHeader, encodedPayload])
            
        else:
            raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttSubAckMsg._name_, mqttSubAckMsg._class_._name_))
    
    def _encodeFixedHeaderAndMessageIdOnlyMsg(self, mqttMessage):
        msgType = mqttMessage.fixedHeader.messageType
        if(isinstance(mqttMessage, MqttUnsubscribeMsg) \
            or isinstance(mqttMessage, MqttPubAckMsg)  \
            or isinstance(mqttMessage, MqttPubRecMsg)  \
            or isinstance(mqttMessage, MqttPubCompMsg) \
            or isinstance(mqttMessage, MqttPubRelMsg)):
            fixedHeader = mqttMessage.fixedHeader
            variableHeaderSize = 2
            # Encode Variable Header
            encodedVariableHeader = self._encodeIntShort(mqttMessage.messageId)
            return b''.join([self._encodeFixedHeader(fixedHeader, variableHeaderSize, None), encodedVariableHeader])
        else:
            if msgType == UNSUBACK: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttUnsubAckMsg._name_, mqttMessage._class_._name_))
            if msgType == PUBACK: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubAckMsg._name_, mqttMessage._class_._name_))
            if msgType == PUBREC: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubRecMsg._name_, mqttMessage._class_._name_))
            if msgType == PUBCOMP: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubCompMsg._name_, mqttMessage._class_._name_))
            if msgType == PUBREL: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPubRelMsg._name_, mqttMessage._class_._name_))
    
    def _encodeFixedHeaderOnlyMsg(self, mqttMessage):
        msgType = mqttMessage.fixedHeader.messageType
        if(isinstance(mqttMessage, MqttPingReqMsg)      \
            or isinstance(mqttMessage, MqttPingRespMsg) \
            or isinstance(mqttMessage, MqttDisconnetMsg)):
            return self._encodeFixedHeader(mqttMessage.fixedHeader, 0, None)
        else:
            if msgType == PINGREQ: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPingReqMsg._name_, mqttMessage._class_._name_))
            if msgType == PINGRESP: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttPingRespMsg._name_, mqttMessage._class_._name_))
            if msgType == DISCONNECT: raise TypeError('MqttEncoder: Expecting message object of type %s got %s' % (MqttDisconnetMsg._name_, mqttMessage._class_._name_))
    
    def _encodeFixedHeader(self, fixedHeader, variableHeaderSize, encodedPayload):
        encodedFixedHeader = bytearray()
        if encodedPayload is None:
            length = 0
        else: length = len(encodedPayload)
        encodedRemainingLength = self._encodeRemainingLength(variableHeaderSize + length)
        encodedFixedHeader.append(self._getFixedHeaderFirstByte(fixedHeader))
        encodedFixedHeader.extend(encodedRemainingLength)
        return encodedFixedHeader
    
    def _getFixedHeaderFirstByte(self, fixedHeader):
        firstByte = fixedHeader.messageType
        if (fixedHeader.dup):
            firstByte |= 0x08;
        firstByte |= fixedHeader.qos << 1;
        if (fixedHeader.retain):
            firstByte |= 0x01;
        return firstByte;
    
    def _encodeRemainingLength(self, num):
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
    
    def _encodeSignedChar(self, number): 
        return struct.pack("!b", number)

    def _encodeIntShort(self, number): 
        return struct.pack("!H", number)
    
    def _encodeStringUtf8(self, s):
        return s.encode('utf-8')
        
    def _isValidClientId(self, clientId):   
        if (clientId is None):
            return 0
        length = len(clientId)
        return length >= 1 and length <= 23
