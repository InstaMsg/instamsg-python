from logging import Handler


class ServerLogHandler(Handler):

    def __init__(self, instamsg):
        Handler.__init__(self)

        self.instamsg = instamsg

    def emit(self, record):
        msg = self.format(record)
        if self.instamsg.enableLogToServer() and self.instamsg.connected():
            #Logging should always send QOS 0 else PubAck can trigger a recursive cycle
            self.instamsg.publish(self.instamsg.serverLogsTopic, msg, 0, 0, logging=0)
