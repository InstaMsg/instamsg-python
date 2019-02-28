from logging import StreamHandler


class ServerLogHandler(StreamHandler):

    def __init__(self, instamsg):
        StreamHandler.__init__(self)

        self.instamsg = instamsg

    def emit(self, record):
        msg = self.format(record)
        if self.instamsg.enableLogToServer() and self.instamsg.connected():
            self.instamsg.publish(self._serverLogsTopic, msg, 1, 0, logging=0)
