import socket
import websocket
from websocket import WebSocket
from websocket._abnf import ABNF
import traceback

from .errors import *
from .constants import *
from .client import MqttClient


class MqttClientWebSocket(MqttClient):

    def _getDataFromSocket(self):
        try:
            return self._sock.recv()    
        except (websocket.WebSocketTimeoutException, socket.timeout):                
            pass
        except Exception as e:
            self._resetSock()
            self._log(MQTT_LOG_LEVEL_DEBUG, "[MqttClientWebSocket]:: Error in receive: %s .Connection reset." % (str(e)))
        

    def _sendallDataToSocket(self, data):
        try:
            self._sock.send(data, opcode=ABNF.OPCODE_BINARY) 
        except (websocket.WebSocketTimeoutException, socket.timeout) as e:                
            raise socket.timeout(str(e))  
        except Exception as e:                  
            self._resetSock()
            raise socket.error(str("[MqttClientWebSocket]:: Error in send: %s. Connection reset." % (str(e))))


    def _setSocketNConnect(self):  
        if(self._logLevel == MQTT_LOG_LEVEL_DEBUG):     
            websocket.enableTrace(True)
        sockopt=((socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),)
        url = "ws://%s:%s/" % (self.host, self.port)
        self._sock = websocket.WebSocket(skip_utf8_validation = False, 
                                        sockopt = sockopt, 
                                        enable_multithread = True)
        self._sock.settimeout(MQTT_SOCKET_TIMEOUT) 
        self._log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Opening web socket to %s' % url)     
        self._sock.connect(url) 
        self._log(MQTT_LOG_LEVEL_INFO, '[MqttClient]:: Web socket opened to %s' % url) 
