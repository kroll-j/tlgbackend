from crud_fuzzer import *
from gp.client import *
from test_config import *
import sys
import threading
import random
import time

class KillerThread ( threading.Thread ):
    """randomly kills client socket"""
    
    def __init__(self, fuzzer):
        super(KillerThread, self).__init__()
        
        self.stopped = False
        self.fuzzer = fuzzer
        self.delay = (0.01, 0.2)
        
    def kill_connection(self):
        if ( self.fuzzer.gp 
            and self.fuzzer.gp.transport.socket ):
                
            try:
                self.fuzzer.kill_lock.acquire()
                
                self.fuzzer.gp.transport.hin.close()
                self.fuzzer.gp.transport.hout.close()
                self.fuzzer.gp.transport.socket.close()
                
                self.fuzzer.killed = True
                self.fuzzer.blip("!")
            finally:
                self.fuzzer.kill_lock.release()
        
    def run ( self ):
        while not self.stopped:
            d = random.random() * ( self.delay[1] - self.delay[0] ) + self.delay[0]
            time.sleep( d )
            
            if not self.stopped:
                self.kill_connection()
            
    def stop(self):
        self.stopped = True
            
      
class TcpFuzzer (CrudFuzzer):
    """Test server stability with unstable TCP client connection"""
    
    def __init__(self):
        super(TcpFuzzer, self).__init__()
        
        self.kill_lock = threading.Lock()
        
        self.killer = KillerThread( self )
        self.killer.daemon = True
        
        self.killed = False

    def run(self, argv):
        try:
            super(TcpFuzzer, self).run(argv)
        finally:
            self.killer.stop()
        
    def connect(self):
        super(TcpFuzzer, self).connect()
        
        # force 1 byte chunks, to increase the probability of 
        # incomplete writes.
        self.gp.transport.chunk_size = 1 
        
    def doFuzz(self):
        try:
            self.kill_lock.acquire()
            
            if not self.killer.is_alive():
                self.killer.start()

            if self.killed:
                self.connect()
                self.prepare()
                self.killed = False
                
                self.blip("*")
        finally:
            self.kill_lock.release()
            
        try:
            return super(TcpFuzzer, self).doFuzz()
        except:
            pass #whatever
            
        return False

if __name__ == '__main__':
    fuzzer = TcpFuzzer()

    fuzzer.run(sys.argv)
