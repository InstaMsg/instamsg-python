####Mqtt Codec ###############################################################################

from .constants import *
from .errors import *
from .messages import *

class MqttDecoder:
    READING_FIXED_HEADER_FIRST = 0
    READING_FIXED_HEADER_REMAINING = 1
    READING_VARIABLE_HEADER = 2
    READING_PAYLOAD = 3
    DISCARDING_MESSAGE = 4
    MESSAGE_READY = 5
    BAD = 6
    
    def __init__(self):
        self.__data = b''
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
        self.__variableHeader['topicLength'] = None
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
            if (self.__fixedHeader.qos > MQTT_QOS0 and self.__variableHeader['topic'] is not None and self.__variableHeader['messageId'] is None):
                self.__variableHeader['messageId'] = self.__decodeMsbLsb()
            if (self.__variableHeader['topic'] is not None and (self.__fixedHeader.qos == MQTT_QOS0 or self.__variableHeader['messageId'] is not None)):
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
