#!/usr/bin/python
# task list generator - interface to catgraph
import time
from gp import *
from utils import *

class CatGraphInterface:
    def __init__(self, host='willow.toolserver.org', port=6666, graphname=None):
        self.gp= client.Connection( client.ClientTransport(host, port), graphname )
        self.gp.connect()
        self.graphname= graphname
        self.wikiname= graphname + '_p'
    
    def getPagesInCategory(self, category, depth=2):
        catID= getCategoryID(self.wikiname, category)
        if catID!=None:
            result= []
            # convert list of tuples to simple list. is there a faster (i.e. built-in) way to do this?
            for i in self.gp.capture_traverse_successors(catID, depth):
                result.append(i[0])
            return result
        else:
            # category not found. return empty result. (would it be better to throw an exception here?)
            return []
    
    ## execute a search engine-style string
    #  operators '+' (intersection) and '-' (difference) are supported
    #  e. g. "Biology Art +Apes -Cats" searches for everything in Biology or Art and in Apes, not in Cats
    #  search parameters are evaluated from left to right, i.e. results might differ depending on order.
    #  on the first category, any '+' operator is ignored, while a '-' operator yields an empty result.
    #  the "depth" parameter is applied to each category.
    #  @param string The search string.
    #  @param depth The search depth.
    def executeSearchString(self, string, depth):
        result= set()
        n= 0
        for param in string.split():
            if param[0] in '+-':
                category= param[1:]
                op= param[0]
            else:
                category= param
                op= '|'
            if op=='|':
                result|= set(self.getPagesInCategory(category, depth))
            elif op=='+':
                if n==0:
                    # '+' on first category should do the expected thing
                    result|= set(self.getPagesInCategory(category, depth))
                else:
                    result&= set(self.getPagesInCategory(category, depth))
            elif op=='-':
                # '-' on first category has no effect
                if n!=0:
                    result-= set(self.getPagesInCategory(category, depth))
            n+= 1
        return list(result)
        

if __name__ == '__main__':
    cg= CatGraphInterface(graphname='dewiki')
    depth= 5
    
    t= time.time()
    for category in ['Biologie', 'Katzen', 'Foo', 'Astrobiologie']:
        catID= getCategoryID('dewiki_p', category)
        if catID:
            cg.gp.capture_traverse_successors(catID, depth)
    traw= time.time()-t
    
    search= '+Biologie -Katzen -Astrobiologie Foo'
    print "searching for '%s'..." % search
    sys.stdout.flush()
    t= time.time()
    set= cg.executeSearchString(search, depth)
    #print set
    print "search found %d pages" % (len(set))
    tcooked= time.time()-t
    
    print "traw: %s tcooked: %s" % (traw, tcooked)
    
