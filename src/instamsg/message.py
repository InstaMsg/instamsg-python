# -*- coding: utf-8 -*-

from .constants import *

class Message:
    def __init__(self, messageId, topic, body, qos=INSTAMSG_QOS0, dup=0, replyTopic=None, instaMsg=None):
        self.__instaMsg = instaMsg
        self.__id = messageId
        self.__topic = topic
        self.__body = body
        self.__replyTopic = replyTopic
        self.__responseId = None
        self.__dup = dup
        self.__qos = qos
        
    def id(self):
        return self.__id
    
    def topic(self):
        return self.__topic
    
    def qos(self):
        return self.__qos
    
    def isDublicate(self):
        return self.__dup
    
    def body(self):
        return self.__body
    
    def replyTopic(self):
        return self.__replyTopic
        
    def reply(self, msg, dup=0, replyHandler=None, timeout=INSTAMSG_RESULT_HANDLER_TIMEOUT):
        if(self.__instaMsg and self.__replyTopic):
            msgId = self.__instaMsg._generateMessageId()
            replyMsgJsonString = ('{"message_id": "%s", "response_id": "%s", "reply_to": "%s", "body": "%s", "status": 1}') % (msgId, self.__id, self.__topic, msg)
            self.__instaMsg._send(msgId, self.__replyTopic, replyMsgJsonString, self.__qos, dup, replyHandler, timeout)
    
    def fail(self, errorCode, errorMsg):
        if(self.__instaMsg and self.__replyTopic):
            msgId = self.__instaMsg._generateMessageId()
            failReplyMsgJsonString = ('{"message_id": "%s", "response_id": "%s", "reply_to": "%s", "body": {"error_code":%d, "error_msg":%s}, "status": 0}') % (msgId, self.__id, self.__topic, errorCode, errorMsg)
            self.__instaMsg._send(msgId, self.__replyTopic, failReplyMsgJsonString, self.__qos, 0, None, 0)
    
    def sendFile(self, fileName, resultHandler, timeout):
        pass
    
    def _sendMsgJsonString(self):
        return ('{"message_id": "%s", "reply_to": "%s", "body": "%s"}') % (self.__id, self.__replyTopic, self.__body)
    
    def toString(self):
        return ('[ id=%s, topic=%s, body=%s, qos=%s, dup=%s, replyTopic=%s]') % (str(self.__id), str(self.__topic), str(self.__body), str(self.__qos), str(self.__dup), str(self.__replyTopic))
    
    def __sendReply(self, msg, replyHandler):
        pass