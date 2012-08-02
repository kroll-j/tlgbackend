#!/usr/bin/python
# task list generator - interface to catgraph
from gp import *



class CatGraphInterface:
    def __init__(self, host='willow.toolserver.org', port=6666, graphname=None):
        self.gp= client.Connection( client.ClientTransport(host, port), graphname )
        self.gp.connect()
    
    def getPagesInCategory(self, catID, depth):
        return self.gp.capture_traverse_successors(catID, depth)

if __name__ == '__main__':
    cg= CatGraphInterface(graphname='dewiki')
    print set(cg.getPagesInCategory(306224, 5)) & set(cg.getPagesInCategory(7017447, 5))
    pass
