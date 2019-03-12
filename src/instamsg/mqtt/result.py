# -*- coding: utf-8 -*-
class Result:
    def __init__(self, result, succeeded=1, cause=None):
        self.__result = result
        self.__succeeded = 1
        self.__cause = cause
        
    def result(self):
        return self.__result
    
    def failed(self):
        return not self.__succeeded
    
    def succeeded(self):
        return self.__succeeded
    
    def cause(self):
        return self.__cause