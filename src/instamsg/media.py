# -*- coding: utf-8 -*-

from threading import Thread, Event, RLock 

from .errors import *

class MediaStream(Thread):
    
    def __init__(self, instamsg, uri, clientId, streamId=None, options={}):
        try:
            import gst
            import re
        except ImportError:
            raise Exception("Unable to import required libraries for streaming")
        
        Thread.__init__(self)
        self.name = 'InstaMsg Thread'
        self.alive = Event()
        self.alive.set()
        
        self.gst = gst
        self.re = re
        self.instamsg = instamsg
        self.uri = uri
        self.streamId = streamId;
        self.streamIds =[]
        self.ipAddress = instamsg.ipAddress
        self.port = ''
        self.clientId = clientId
        self.__qos = 1
        self.__dup = 0
        self.__mediaTopic = "instamsg/clients/" + self.clientId + "/media"
        self.__mediaReplyTopic = "instamsg/clients/" + self.clientId + "/mediareply"
        self.__mediaStopTopic = "instamsg/clients/" + self.clientId + "/mediastop"
        self.__mediaPauseTopic = "instamsg/clients/" + self.clientId + "/mediapause"
        self.__mediaStreamsTopic = "instamsg/clients/" + self.clientId + "/mediastreams"
        self.__initStreaming()
        
  
        
    def __initStreaming(self):
        self.__subscribe(self.__mediaReplyTopic, self.__qos)
        time.sleep(30)
        
        self.__publishMediaMessage(self.__mediaTopic)
        # self.__subscribe(self.__mediaPauseTopic, self.__qos)
        self.__subscribe(self.__mediaStopTopic, self.__qos)
        self.__subscribe(self.__mediaStreamsTopic, self.__qos)

    def broadcast(self, sdpAnswer):
        self.__processOffer(sdpAnswer)
 
    def stop(self):
        if (self.pipeline):
                self.pipeline.set_state(self.pipeline.set_state(self.gst.STATE_NULL))
                self.instamsg.log(INSTAMSG_LOG_LEVEL_DEBUG, "Stopping stream and stream id is %s" % str(self.streamId))
        message = {'to':self.clientId,'from':self.clientId,'type':3,'stream_id': self.streamId}
        self.__publish(self.__mediaTopic, str(message), self.__qos, self.__dup)
    
    def paused(self):
        self.pipeline.set_state(self.gst.STATE_PAUSED)
        
    def __publishMediaMessage(self, topic, qos=1, dup=0):
        
        sdpOffer = "v=0\r\n";
        sdpOffer += "o=- 0 0 IN IP4 " + self.ipAddress + "\r\n";
        sdpOffer += "s=\r\n";
        sdpOffer += "c=IN IP4 " + self.ipAddress + "\r\n";
        sdpOffer += "t=0 0\r\n";
        sdpOffer += "a=charset:UTF-8\n";
        sdpOffer += "a=recvonly\r\n";
        sdpOffer += "m=video 50004 RTP/AVP 96\r\n";
        sdpOffer += "a=rtpmap:96 H264/90000\r\n";
        
        message = {
                    'to': self.clientId,
                    'sdp_offer' : sdpOffer,
                    'from': self.clientId,
                    'protocol' : 'rtp',
                    'type':'7',
                    'stream_id':self.streamId,
                    'record': True
                }
        self.__publish(topic, str(message), qos, dup)
        
    def __publish(self, topic, msg, qos, dup):
        try:
            def _resultHandler(result):
                self.instamsg.log(INSTAMSG_LOG_LEVEL_DEBUG, "Published message %s to topic %s with qos %d" % (msg, topic, qos))
            self.instamsg.publish(topic, msg, qos, dup, _resultHandler)
        except Exception as e:
            self.instamsg.log(INSTAMSG_LOG_LEVEL_DEBUG, e)
        
    def __subscribe(self, topic, qos=1):
        try:
            def _resultHandler(result):
                self.instamsg.log(INSTAMSG_LOG_LEVEL_DEBUG, "Subscribed to topic %s with qos %d" % (topic, qos))
            self.instamsg.subscribe(topic, qos, self.__messageHandler, _resultHandler)
        except Exception as e:
            self.instamsg.log(INSTAMSG_LOG_LEVEL_DEBUG, e)
            
    def __messageHandler(self, mqttMessage):
        if(mqttMessage):
            self.instamsg.log(INSTAMSG_LOG_LEVEL_DEBUG, "Media streamer received message %s" % str(mqttMessage.toString()))
            if(mqttMessage.topic() == self.__mediaReplyTopic):
                self.__handleMediaReplyMessage(mqttMessage)
            if(mqttMessage.topic() == self.__mediaPauseTopic):
                self.paused()
            if(mqttMessage.topic() == self.__mediaStopTopic):
                self.stop()
            if(mqttMessage.topic() == self.__mediaStreamsTopic):
                self.__handleMediaStremsMessage(mqttMessage)
    
    def __handleMediaStremsMessage(self, mqttMsg):
        msgJson = self.__parseJson(mqttMsg.body())
        
        self.instamsg.log(INSTAMSG_LOG_LEVEL_DEBUG, msgJson)
        messageId, replyTopic, method = None, None, None
        if('reply_to' in msgJson):
            replyTopic = msgJson['reply_to']
        else:
            raise ValueError("Media stream message json should have reply_to address.") 
        if('message_id' in msgJson):
            messageId = msgJson['message_id']
        else: 
            raise ValueError("Media stream message json should have a message_id.")  
        if('method' in msgJson):
            method = msgJson['method']
        else: 
            raise ValueError("Media stream message json should have a method.")
        if(replyTopic):
            if(method == "GET"):
                msg = '{"response_id": "%s", "status": 1, "streams": "%s"}' % (messageId, str(self.streamIds))
                self.__publish(replyTopic, msg, self.__qos, self.__dup)
        
               
    def __handleMediaReplyMessage(self, mqttMessage):
        msgJson = self.__parseJson(mqttMessage.body())
        if(msgJson is not None and('stream_id' in msgJson)):
            self.streamId = msgJson['stream_id']
            self.streamIds.append(self.streamId)
        if(msgJson is not None and('sdp_answer' in msgJson)):
            self.broadcast(msgJson['sdp_answer'])
            
    def __parseJson(self, jsonString):
        return json.loads(jsonString)
    
    def __processOffer(self, sdpAnswer):
        isAddress = self.re.search('o=- 0 (.+?) IN IP4 (.+?)\r\ns=', sdpAnswer)
        if isAddress:
            self.ipAddress = isAddress.group(2)
            
        isPort = self.re.search('m=video (.+?) RTP/AVP 96', sdpAnswer)
        if isPort:
            self.port = isPort.group(1)

        self.__createStreamingPipline()
     
    def __createStreamingPipline(self):
        add = "23.253.42.123"
        pipe = (self.uri + " !  udpsink host=%s port=%s" % (add, self.port))
        
        self.instamsg.log(INSTAMSG_LOG_LEVEL_DEBUG, "Stream uri is  %s" % str(pipe))
        self.pipeline = self.gst.parse_launch(pipe)
 
        self.pipeline.set_state(self.gst.STATE_PLAYING)
        self.instamsg.log(INSTAMSG_LOG_LEVEL_DEBUG, "Media streaming started on %s" % str(self.port))
