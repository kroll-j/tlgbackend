from fuzzer_base import FuzzerBase, test_graph_name
from gp.client import Connection
from test_config import *
import random
import sys

class ArcFuzzer(FuzzerBase):
    """Tests the TCP client connection"""
 
    def __init__(self):
        self.offset = 1
        FuzzerBase.__init__(self)
    
    def prepare(self):
        # in case we use a persistent graph, fund an unused offset
        Range = range(self.offset,10)
        for i in Range:
            if not self.gp.capture_list_successors(i):
                self.gp.add_arcs(((i, i+1),))
                print "fuzz offset: %d (%s)" % (i, test_graph_name)
                return
            
            self.offset = i + 1
            #? self.offset verstehen!
        
        exit("no free offset left (or "
          + test_graph_name + "needs purging)")
         
    
    def random_node(self):
        return random.randint(10, 1000) * 10 + self.offset
         
    
    def random_arcs(self, n=0):
        if not n:
            n = random.randint(2, 80)
        arcs = []
        for i in range(0, n):
            a = self.random_node()
            b = self.random_node()
            arcs.append((a, b))
        return arcs
    
    def random_set(self, n=0):
        if not n:
            n = random.randint(2, 80)
        arcs = []
        for i in range(0, n):
            x = self.random_node()
            arcs.append(x)
        return arcs
    
    def doFuzz(self):
        self.gp.add_arcs(self.random_arcs())
        self.gp.remove_arcs(self.random_arcs())
        
        self.gp.replace_successors(self.random_node(), self.random_set())
        self.gp.replace_predecessors(self.random_node(), self.random_set())
        
        return True
         
    
if __name__ == '__main__':

    fuzzer = ArcFuzzer()

    fuzzer.run(sys.argv)
