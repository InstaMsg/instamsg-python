# -*- coding: utf-8 -*-

class InstaMsgError(Exception):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class InstaMsgSubError(InstaMsgError):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class InstaMsgUnSubError(InstaMsgError):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class InstaMsgPubError(InstaMsgError):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)
    
class InstaMsgSendError(InstaMsgError):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)

class InstaMsgProvisionError(InstaMsgError):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)

class InstaMsgProvisionTimeout(InstaMsgProvisionError):
    def __init__(self, value=''):
        self.value = value
    def __str__(self):
        return repr(self.value)