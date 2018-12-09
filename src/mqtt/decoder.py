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
        self._data = b''
        self._init()
        self._msgFactory = MqttMsgFactory()
        
    def _state(self):
        return self._state
    
    def decode(self, data=b''):
        if(data or self._data ):
            self._data = self._data.join([data])
            if(self._state == self.READING_FIXED_HEADER_FIRST):
                self._decodeFixedHeaderFirstByte(self._getByteStr())
                self._state = self.READING_FIXED_HEADER_REMAINING
            if(self._state == self.READING_FIXED_HEADER_REMAINING):
                self._decodeFixedHeaderRemainingLength()
                if (self._fixedHeader.messageType == PUBLISH and not self._variableHeader):
                    self._initPubVariableHeader()
            if(self._state == self.READING_VARIABLE_HEADER):
                self._decodeVariableHeader()
            if(self._state == self.READING_PAYLOAD):
                bytesRemaining = self._remainingLength - (self._bytesConsumedCounter - self._remainingLengthCounter - 1)
                self._decodePayload(bytesRemaining)
            if(self._state == self.DISCARDING_MESSAGE):
                bytesLeftToDiscard = self._remainingLength - self._bytesDiscardedCounter
                if (bytesLeftToDiscard <= len(self._data)):
                    bytesToDiscard = bytesLeftToDiscard
                else: bytesToDiscard = len(self._data)
                self._bytesDiscardedCounter = self._bytesDiscardedCounter + bytesToDiscard
                self._data = self._data[0:(bytesToDiscard - 1)] 
                if(self._bytesDiscardedCounter == self._remainingLength):
                    e = self._error
                    self._init()
                    # Discard any data that is there.
                    self._data = b''
                    raise MqttDecoderError(e) 
            if(self._state == self.MESSAGE_READY):
                # returns a tuple of (mqttMessage, dataRemaining)
                mqttMsg = self._msgFactory.message(self._fixedHeader, self._variableHeader, self._payload)
                self._init()
                return mqttMsg
            if(self._state == self.BAD):
                # Discard any data that is there.
                e = self._error
                self._init()
                raise MqttFrameError(e)  
        return None 
            
    def _decodeFixedHeaderFirstByte(self, byteStr):
        byte = ord(byteStr)
        self._fixedHeader.messageType = (byte & 0xF0)
        self._fixedHeader.dup = (byte & 0x08) >> 3
        self._fixedHeader.qos = (byte & 0x06) >> 1
        self._fixedHeader.retain = (byte & 0x01)
    
    def _decodeFixedHeaderRemainingLength(self):
            while (self._data):
                byte = ord(self._getByteStr())
                self._remainingLength += (byte & 127) * self._multiplier
                self._multiplier *= 128
                self._remainingLengthCounter = self._remainingLengthCounter + 1
                if(self._remainingLengthCounter > 4):
                    self._state = self.BAD
                    self._error = ('MqttDecoder: Error in decoding remaining length in message fixed header.') 
                    break
                if((byte & 128) == 0):
                    self._state = self.READING_VARIABLE_HEADER
                    self._fixedHeader.remainingLength = self._remainingLength
                    break
                
    def _initPubVariableHeader(self):
        self._variableHeader['topicLength'] = None
        self._variableHeader['messageId'] = None
        self._variableHeader['topic'] = None
        

    def _decodeVariableHeader(self):
        if self._fixedHeader.messageType in [CONNECT, SUBSCRIBE, UNSUBSCRIBE, PINGREQ]:
            self._state = self.DISCARDING_MESSAGE
            self._error = ('MqttDecoder: Client cannot receive CONNECT, SUBSCRIBE, UNSUBSCRIBE, PINGREQ message type.') 
        elif self._fixedHeader.messageType == CONNACK:
            if(self._fixedHeader.remainingLength != 2):
                self._state = self.BAD
                self._error = ('MqttDecoder: Mqtt CONNACK message should have remaining length 2 received %s.' % self._fixedHeader.remainingLength) 
            elif(len(self._data) < 2):
                pass  # let for more bytes come
            else:
                self._getByteStr()  # discard reserved byte
                self._variableHeader['connectReturnCode'] = ord(self._getByteStr())
                self._state = self.MESSAGE_READY
        elif self._fixedHeader.messageType == PROVACK:
            if(len(self._data) < 2):
                pass  # let for more bytes come
            else:
                self._getByteStr()  # discard reserved byte
                self._variableHeader['provisionReturnCode'] = ord(self._getByteStr())
                self._state = self.READING_PAYLOAD        
        elif self._fixedHeader.messageType == SUBACK:
            messageId = self._decodeMsbLsb()
            if(messageId is not None):
                self._variableHeader['messageId'] = messageId
                self._state = self.READING_PAYLOAD
        elif self._fixedHeader.messageType in [UNSUBACK, PUBACK, PUBREC, PUBCOMP, PUBREL]:
            messageId = self._decodeMsbLsb()
            if(messageId is not None):
                self._variableHeader['messageId'] = messageId
                self._state = self.MESSAGE_READY
        elif self._fixedHeader.messageType == PUBLISH:
            if(self._variableHeader['topic'] is None):
                self._decodeTopic()
            if (self._fixedHeader.qos > MQTT_QOS0 and self._variableHeader['topic'] is not None and self._variableHeader['messageId'] is None):
                self._variableHeader['messageId'] = self._decodeMsbLsb()
            if (self._variableHeader['topic'] is not None and (self._fixedHeader.qos == MQTT_QOS0 or self._variableHeader['messageId'] is not None)):
                self._state = self.READING_PAYLOAD
        elif self._fixedHeader.messageType in [PINGRESP, DISCONNECT]:
            self._mqttMsg = self._msgFactory.message(self._fixedHeader)
            self._state = self.MESSAGE_READY
        else:
            self._state = self.DISCARDING_MESSAGE
            self._error = ('MqttDecoder: Unrecognised message type - %s' % str(self._fixedHeader.messageType)) 
            
    def _decodePayload(self, bytesRemaining):
        paloadBytes = self._getNBytesStr(bytesRemaining)
        if(paloadBytes is not None):
            if self._fixedHeader.messageType == SUBACK:
                grantedQos = []
                numberOfBytesConsumed = 0
                while (numberOfBytesConsumed < bytesRemaining):
                    qos = int(ord(paloadBytes[numberOfBytesConsumed]) & 0x03)
                    numberOfBytesConsumed = numberOfBytesConsumed + 1
                    grantedQos.append(qos)
                self._payload = grantedQos
                self._state = self.MESSAGE_READY
            elif self._fixedHeader.messageType in (PUBLISH, PROVACK):
                self._payload = paloadBytes.decode('utf-8')
                self._state = self.MESSAGE_READY
    
    def _decodeTopic(self):
        stringLength = self._variableHeader['topicLength']
        if(stringLength is None):
            stringLength = self._decodeMsbLsb()
            self._variableHeader['topicLength'] = stringLength
        if (self._data and stringLength and (len(self._data) < stringLength)):
            return None  # wait for more bytes
        else:
            self._variableHeader['topic'] = self._getNBytesStr(stringLength).decode('utf-8')
    
    def _decodeMsbLsb(self):
        if(len(self._data) < 2):
            return None  # wait for 2 bytes
        else:
            msb = self._getByteStr()
            lsb = self._getByteStr()
            intMsbLsb = ord(msb) << 8 | ord(lsb)
        if (intMsbLsb < 0 or intMsbLsb > MQTT_MAX_INT):
            return - 1
        else:
            return intMsbLsb
        
    
    def _getByteStr(self):
        return self._getNBytesStr(1)
    
    def _getNBytesStr(self, n):
        # gets n or less bytes
        nBytes = self._data[0:n]
        self._data = self._data[n:len(self._data)]
        self._bytesConsumedCounter = self._bytesConsumedCounter + n
        return nBytes
    
    def _init(self):
        self._state = self.READING_FIXED_HEADER_FIRST
        self._remainingLength = 0
        self._multiplier = 1
        self._remainingLengthCounter = 0
        self._bytesConsumedCounter = 0
        self._payloadCounter = 0
        self._fixedHeader = MqttFixedHeader()
        self._variableHeader = {}
        self._payload = None
        self._mqttMsg = None
        self._bytesDiscardedCounter = 0 
        self._error = 'MqttDecoder: Unrecognized _error'
