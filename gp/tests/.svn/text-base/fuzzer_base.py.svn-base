from gp.client import Connection
from gp.client import gpException
from test_config import *
import test_config
import os, sys
import random
import time

test_graph_name = 'test' + str(os.getpid())

def fuzz_pick( a ):
    i = random.randint(0, len(a)-1)
    return a[i]
     

class FuzzerBase (object): # abstract
    """Test the TCP client connection"""
    
    def __init__(self):
        self.graph = None
        self.useTempGraph = True
        
    def blip( self, s ):
		sys.stdout.write( s )
		sys.stdout.flush()
    
    def newConnection(self):
        gp = Connection.new_client_connection(None,
          test_graphserv_host, test_graphserv_port )
        gp.connect()
        return gp
    
    def connect(self):
        if not self.graph:
            self.graph = test_graph_name
        
        try:
            self.gp = self.newConnection()
        except gpException as ex:
            print("Unable to connect to "
              + test_graphserv_host + ":" + str(test_graphserv_port)
              + ", please make sure the graphserv process is running "
              + "and check the test_graphserv_host and "
              + "test_graphserv_port configuration options in "
              + "test_config.py.")
            print("Original error: " + str(ex))
            quit(11)
        
        try:
            self.gp.authorize( 'password',
              test_admin + ":" + test_admin_password)
        except gpException, ex:
            print("Unable to connect to authorize as "
              + test_admin + ", please check the test_admin and "
              + "test_admin_password configuration options in "
              + "test_config.py.")
            print("Original error: " + str(ex))
            quit(12)
        
        if self.useTempGraph:
            self.gp.try_create_graph( self.graph )
        
        try:
            self.gp.use_graph( self.graph )
        except gpException, ex:
            print("Unable to use graph self.graph, please check the "
              + "test_graph_name configuration option in test_config.py "
              + "as well as the privileges of user " + test_admin + ".")
            print("Original error: " + ex.getMessage())
            quit(13)
    
    def disconnect(self):
        global test_admin, test_admin_password

        if self.useTempGraph and self.graph:
            self.gp.try_drop_graph(self.graph) #? gp OK?
    
    def prepare(self):
        pass #noop
    
    def doFuzz(self): #abstract
        raise NotImplementedError(
          "FuzzerBase.doFuzz() not implemented.")
    
    def run(self, argv):
        self.connect()
        self.prepare()
        
        n = None
        if len(argv) > 1:
            n = int(argv[1])
        if not n:
            n = 100

        for k in range(n):
            for i in range(100):
                ok = self.doFuzz()
                if ok:
                    self.blip("+")
                else:
                    self.blip("-")
            
            self.blip("\n");
            # time.sleep(1) #? Muss wieder rein!
        
        self.disconnect()
