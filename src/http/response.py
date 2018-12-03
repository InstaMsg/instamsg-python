# -*- coding: utf-8 -*-
####HttpClient ###############################################################################    

from .errors import *
    
class HTTPResponse:
    __blanklines = ('\r\n', '\n', '') 
    __crlf = '\r\n'
    __continuationChar = '\t'
    __whitespace = " " 
    __readingStatusline = 0
    __readingHeaders = 1
    __readingBody = 2
    __ok = 3
    __continue = 100
    
    def __init__(self, sock, f=None):
        self.__sock = sock
        self.f = f

        
    def response(self):  
        try:
            self.__init()
#             data_block = self.__sock.recv()
            data_block = self.__sock.recv(1500)
            while(data_block):
                self.__lines = self.__lines + data_block.split(self.__crlf)
                if(len(self.__lines) > 0):
                    if(self.state == self.__readingStatusline):
                        self.__readStatus()
                        # read till we get a non conitnue (100) response
                        if(self.__state == self.__continue): 
                            self.__state = self.__readingStatusline
                            break
#                             data_block = self.__sock.recv(1500)
                    if(self.__state == self.__readingHeaders):
                        self.__readHeaders()
                    if(self.__readingBody):
                        self.__readBody() 
                        break
                if(self.__sock is not None):
#                     data_block = self.__sock.recv()
                    data_block = self.__sock.recv(1500)
                if not data_block:
                    break
        except Exception as e:
            raise HTTPResponseError(str(e))
        return self

    def end(self):
        try:
            if(self.__sock):
                self.__sock.close()
                self.__sock = None
        except:
            pass
        
    def __init(self):
        self.protocol = None
        self.version = None
        self.status = None
        self.reason = None
        self.length = None
        self.close = None 
        self.headers = {}
        self.body = ""  
        self.state = self.__readingStatusline
        self.__lines = []
        self.__lastHeader = None
        
    def __readStatus(self):
        try:
            statusLine = self.__lines.pop(0)
            [version, status, reason] = statusLine.split(None, 2)
        except ValueError:
            try:
                [version, status] = statusLine.split(None, 1)
                reason = ""
            except ValueError:
                version = ""
        if not version.startswith('HTTP'):
            raise HTTPResponseError("Invalid HTTP version in response")
        try:
            status = int(status)
            if status < 100 or status > 999:
                raise HTTPResponseError("HTTP status code out of range 100-999.")
        except ValueError:
            raise HTTPResponseError("Invalid HTTP status code.")
        self.status = status
        self.reason = reason.strip()
        try:
            [protocol, ver] = version.split("/", 2)
        except ValueError:
            raise HTTPResponseError("Invalid HTTP version.")
        self.protocol = protocol
        self.version = ver
        if(self.status == self.__continue): 
            self.__state = self.__continue
        else:
            self.__state = self.__readingHeaders
            
        
    def __readHeaders(self):
        n = len(self.__lines)
        i = 0
        while i < n:
            line = self.__lines.pop(0)
            if(self.__islastLine(line)):
                self.state = self.__readingBody
                break
            if(self.__isContinuationLine(line)):
                [a, b] = line.split(self.__continuationChar, 2)
                self.headers[self.__lastHeader].append(b.strip()) 
            else:
                headerTuple = self.__getHeader(line)
                if(headerTuple):
                    self.headers[headerTuple[0]] = headerTuple[1]
                    self.__lastHeader = headerTuple[0]
            i = i + 1
            
    def __islastLine(self, line):
        return line in self.__blanklines
    
    def __isContinuationLine(self, line):
        if(line.find(self.__continuationChar) > 0): return 1
        else: return 0
    
    def __getHeader(self, line):
        i = line.find(':')
        if i > 0:
            header = line[0:i].lower()
            if(i == len(line)):
                headerValue = []
            else:
                headerValue = line[(i + 1):len(line)].strip()
            if(header == 'content-length' and headerValue):
                try:
                    self.length = int(headerValue)
                except ValueError:
                    self.length = None
                else:
                    if self.length < 0:
                        self.length = None   
            return (header, headerValue)
        return None
    
    def __readBody(self):
        try:
            try:
                if(self.length and self.length != 0 and (self.__lines or self.__sock)):
        #             datablock = self.__sock.recv()
                    if(self.__lines):
                        datablock = self.__lines
                        self.__lines = None
                    else:
                        datablock = self.__sock.recv(1500)
                    length = 0
                    while(datablock and length < self.length):
                        if(isinstance(datablock, list)):
                            datablock = ''.join(datablock)
                        length = length + len(datablock)
                        # Only download body to file if status 200
                        if (self.status == 200 and self.f and hasattr(self.f, 'write')):  
                            self.f.write(datablock)
                        else:
                            self.body = self.body + datablock
    #                     datablock = self.__sock.recv()
                        if(length < self.length):
                            datablock = self.__sock.recv(1500)
                        else: break
                    self.end()
                else:
                    self.end()
            except Exception as e:
                raise Exception(str(e))
        finally:
            self.end()