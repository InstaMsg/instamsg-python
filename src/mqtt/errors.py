# -*- coding: utf-8 -*-

class MqttClientError(Exception):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)

class MqttConnectError(Exception):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
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