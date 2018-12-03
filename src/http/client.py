# -*- coding: utf-8 -*-

import os
import socket
import OpenSSL.crypto

from .errors import *

class HTTPClient:
        
    def __init__(self, host, port, userAgent='InstaMsg', enableSsl=0):
        self.version = '1.1'
        self.__userAgent = userAgent
        self.__addr = (host, port)
        self.__sock = None
        self.__checkAddress()
        self.__boundary = '-----------ThIs_Is_tHe_bouNdaRY_78564$!@'
        self.__tcpBufferSize = 1500
        self.__enableSsl = enableSsl
        
    def get(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('GET', url, params, headers, body, timeout)
    
    def put(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('PUT', url, params, headers, body, timeout)
    
    def post(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('POST', url, params, headers, body, timeout)  
    
    def delete(self, url, params={}, headers={}, body=None, timeout=10):
        return self.__request('DELETE', url, params, headers, body, timeout) 
        
    def uploadFile(self, url, filename, params={}, headers={}, timeout=10):
        filename = str(filename)
        if(not isinstance(filename, str)): raise ValueError('HTTPClient:: upload filename should be of type str.')
        f = None
        try:
            try:
                headers['Content-Type'] = 'multipart/form-data; boundary=%s' % self.__boundary
                form = self.__encode_multipart_fileupload("file", filename)
                fileSize = self.__getFileSize(filename)
                headers['Content-Length'] = len(''.join(form)) + fileSize
                f = open(filename, 'rb')
                return self.__request('POST', url, params, headers, f, timeout, form)  
            except Exception as e:
                if(e.__class__.__name__ == 'HTTPResponseError'):
                    raise HTTPResponseError(str(e))
                raise HTTPClientError("HTTPClient:: %s" % str(e))
        finally:
            self.__closeFile(f)  
    
    def downloadFile(self, url, filename, params={}, headers={}, timeout=10):  
        filename = str(filename)
        url = str(url)
        if(not isinstance(filename, str)): raise ValueError('HTTPClient:: download filename should be of type str.')
        f = None
        response = None
        try:
            try:
                tempFileName = '~' + filename
                f = open(tempFileName, 'wb')
                response = self.__request('GET', url, params, headers, timeout=timeout, fileObject=f)
                f.close()
                if(response.status == 200):
    #               rename(tempFileName, filename)
                    os.rename(tempFileName, filename)
                else:
    #                 unlink(tempFileName)
                    os.remove(tempFileName)
            except Exception as e:
                if(e.__class__.__name__ == 'HTTPResponseError'):
                    raise HTTPResponseError(str(e))
                raise HTTPClientError("HTTPClient:: %s" % str(e))
        finally:
            if f:
                self.__closeFile(f)
        return response
            
    def __closeFile(self, f):   
        try:
            if(f and hasattr(f, 'close')):
                f.close() 
                f = None      
        except:
            pass 
          
    def __getFileSize(self, filename):
        fileSize = None
        try:
            try:
                f = open(filename, 'ab')
                f.seek(0, 2)
                fileSize = f.tell()
            except Exception as e:
                raise Exception(str(e))
        finally:
            if(f and hasattr(f, 'close')):
                f.close()
                f = None
        if(fileSize): return fileSize
        else: raise Exception("HTTPClient:: Unable to determine file size.")
            
    
    def __request(self, method, url, params, headers, body=None, timeout=10, fileUploadForm=None, fileObject=None):
        if(not isinstance(url, str)): raise ValueError('HTTPClient:: url should be of type str.')
        if(not isinstance(params, dict)): raise ValueError('HTTPClient:: params should be of type dictionary.')
        if(not isinstance(headers, dict)): raise ValueError('HTTPClient:: headers should be of type dictionary.')
        if(not isinstance(timeout, int)): raise ValueError('HTTPClient:: timeout should be of type int.')
        if(not(isinstance(body, str) or isinstance(body, file) or body is None)):raise ValueError('HTTPClient:: body should be of type string or file object.')
        try:
            try:
                request = self.__createHttpRequest(method, url, params, headers)
                sizeHint = None
                if('Content-Length' in headers and isinstance(body, file)):
                    sizeHint = len(request) + headers.get('Content-Length')
                self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if (self.__enableSsl):
                    self.__sock = ssl.wrap_socket(self.__sock, cert_reqs=ssl.CERT_NONE)
                self.__sock.settimeout(timeout)
                self.__sock.connect(self.__addr)
                expect = None
                if('Expect' in headers):
                    expect = headers['Expect']
                elif('expect' in headers):
                    expect = headers['expect']
                if(expect and (expect.lower() == '100-continue')):
                    self.__sock.sendall(request)
                    httpResponse = HTTPResponse(self.__sock, fileObject).response()
                    # Send the remaining body if status 100 received or server that send nothing
                    if(httpResponse.status == 100 or httpResponse.status is None):
                        request = ""
                        self.__send(request, body, fileUploadForm, fileObject, sizeHint)
                        return httpResponse.response()
                    else:
                        raise HTTPResponseError("Expecting status 100, recieved %s" % request.status)
                else:
                    self.__send(request, body, fileUploadForm, fileObject, sizeHint)
                    return HTTPResponse(self.__sock, fileObject).response()
            except Exception as e:
                if(e.__class__.__name__ == 'HTTPResponseError'):
                    raise HTTPResponseError(str(e))
                raise HTTPClientError("HTTPClient:: %s" % str(e))
        finally:
            try:
                if(self.__sock):
                    self.__sock.close()
                    self.__sock = None
            except:
                pass
    
    def __send(self, request, body=None, fileUploadForm=None, fileObject=None, sizeHint=None):
        if (isinstance(body, str) or body is None): 
            request = request + (body or "")
            if(request):
                self.__sock.sendall(request)
        else:
            if(fileUploadForm and len(fileUploadForm) == 2):
                blocksize = 1500    
                if(sizeHint <= self.__tcpBufferSize):
                    if hasattr(body, 'read'): 
                        request = request + ''.join(fileUploadForm[0]) + ''.join(body.read(blocksize)) + ''.join(fileUploadForm[1])
                        self.__sock.sendall(request)
                else:
                    request = request + ''.join(fileUploadForm[0])
                    self.__sock.sendall(request)
                    partNumber = 1
                    if hasattr(body, 'read'): 
                        partData = body.read(blocksize)
                        while partData:
        #                             self.__sock.sendMultiPart(partData, partNumber)
                            self.__sock.sendall(partData)
                            partData = body.read(blocksize)
                    if(fileUploadForm and len(fileUploadForm) == 2):
        #                         self.__sock.sendMultiPart(fileUploadForm[1], partNumber + 1)
                        self.__sock.sendall(fileUploadForm[1])
        #                 self.__sock.sendHTTP(self.__addr, request)
    
    def __createHttpRequest(self, method, url, params={}, headers={}):
        url = url + self.__createQueryString(params)
        headers = self.__createHeaderString(headers)
        request = "%s %s %s" % (method, url, headers)
        return request
        
    def __createQueryString(self, params={}):
        i = 0
        query = ''
        for key, value in params.items():
            if(i == 0): 
                query = query + '?%s=%s' % (str(key), str(value))
                i = 1
            else:
                query = query + "&%s=%s" % (str(key), str(value))
        return query
    
    def __createHeaderString(self, headers={}): 
            if(self.__enableSsl):
                httpScheme = "HTTPS"
            else:
                httpScheme = "HTTP"
            headerStr = "%s/%s\r\nHost: %s\r\n" % (httpScheme, self.version, self.__addr[0])
            headers['Connection'] = 'close'  # Only close is supported
            headers['User-Agent'] = self.__userAgent
            for header, values in headers.items():
                if(isinstance(values, list)):
                    headerStr = headerStr + "%s: %s\r\n" % (header, '\r\n\t'.join([str(v) for v in values]))
                else:
                    headerStr = headerStr + "%s: %s\r\n" % (str(header), str(values))
            return headerStr + "\r\n"
        
    def __encode_multipart_fileupload(self, fieldname, filename, contentType='application/octet-stream'):
        formPrefix = []
        crlf = '\r\n'
        formPrefix.append("--" + self.__boundary)
        formPrefix.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (fieldname, filename))
        formPrefix.append('Content-Type: %s' % contentType)
        formPrefix.append('')
        formPrefix.append('')
        return (crlf.join(formPrefix), (crlf + '--' + self.__boundary + '--' + crlf))
            
    def __checkAddress(self):
        if (not self.__addr[0] and not self.__addr[1] and not isinstance(self._addr[1], int)):
            raise ValueError("HTTPClient:: Not a valid HTTP host or port value: %s, %d" % (self.__addr[0], self.__addr[1]))
        