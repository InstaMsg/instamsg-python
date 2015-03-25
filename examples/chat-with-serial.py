import instamsg
import sys
import time
import serial

SER = None
ComPort =1
BaudRate =9600
Parity = serial.PARITY_NONE
ClientId = "Your InstaMsg client id"
AuthKey = "Your InstaMsg auth key"

def start(args):
    instaMsg = None
    global SER
    try:
        try:
            options={'logLevel':instamsg.INSTAMSG_LOG_LEVEL_DEBUG, 'enableSsl':1}
            instaMsg = instamsg.InstaMsg(ClientId, AuthKey, __onConnect, __onDisConnect, __oneToOneMessageHandler, options)
            SER = __openSerialPort()
            while 1:
                instaMsg.process()
                time.sleep(1)
        except:
            print("Unknown Error in start: %s %s" %(str(sys.exc_info()[0]),str(sys.exc_info()[1])))
    finally:
        if(instaMsg):
            __closeSerialPort()
            instaMsg.close()
            instaMsg = None
    
def __onConnect(instaMsg):
    print "Client connected to Instamsg"
    
def __onDisConnect():
    print "Client disconnected."
    
      
def __messageHandler(mqttMessage):
        if(mqttMessage):
            print "Received message %s" %str(mqttMessage.toString())
        
def __oneToOneMessageHandler(msg):
    if(msg):
        print "One to One Message received %s" % msg.toString()
        msgBytes = __unhexelify(msg.body())
        __writeToSerialPort(msgBytes)
        time.sleep(1)
        read_val = __readFromSerialPort(1000)
        print "Read data:%s" %str(read_val)
        msg.reply(__hexlify(read_val))
        
def __hexlify(data):
        a = []
        for x in data:
            a.append("%02X" % (ord(x)))
        return ''.join(a)
    
def __unhexelify(data):
        a = []
        for i in range(0, len(data), 2):
            a.append(chr(int(data[i:i + 2], 16)))   
        return ''.join(a) 

def __openSerialPort():
    try:
        print "Configuring serial port..."
        SER = serial.Serial(ComPort, BaudRate, timeout=1, parity=Parity, rtscts=0)
        SER.close()
        print "Opening serial port..."
        SER.open()
        print "Serial port opened..."
        return SER
    except Exception, e:
        print e
        print " handle port in use or invalid name ..." 

def __readFromSerialPort(max_bytes):
    try:
        print "Reading from serial port..."
        return SER.read(size=max_bytes)
    except OSError, e:
        print "... handle No data received or response timeout here ... "
 

def __writeToSerialPort(data):
    try:
        print "Writing to serial port..."
        print SER
        count = SER.write(data)
    except:
        print " ...Not able to write data ... "
        
def __closeSerialPort():
    try:
        SER.close()
    except:
        pass # ignore errors here    
    
if  __name__ == "__main__":
    rc = start(sys.argv)
    sys.exit(rc)