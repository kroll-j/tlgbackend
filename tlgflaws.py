#!/usr/bin/python
# task list generator - flaws
import os
import sys
import time
import json
import Queue
import random
import caching
import tlgbackend
from utils import *

# base class for flaw finders
class FlawFinder:
    def __init__(self, shortname, description):
        self.shortname= shortname
        self.description= description
    
    # this method should return an object of some class derived from TlgAction
    def createAction(self, wiki, pages):
        raise NotImplementedError("execute() not implemented")


# base class for actions to be executed by task list generator
class TlgAction:
    def __init__(self, parent, wiki, pages):
        self.parent= parent
        self.wiki= wiki
        self.pages= pages
    
    def execute(self, resultQueue):
        raise NotImplementedError("execute() not implemented")


# the result of a TlgAction, describing a flawed article
class TlgResult:
    # page: the full page result, i.e. caching.PageIDCache.values
    def __init__(self, wiki, page, flawfinder):
        self.wiki= wiki
        self.page= page
        self.flawfinder= flawfinder
    
    def encodeAsJSON(self):
        return json.dumps( { 'wiki': self.wiki, 'page': self.page, 'ffname': self.flawfinder.shortname, 'ffdesc': self.flawfinder.description } )

# an example flaw finder which does nothing
class FFNop(FlawFinder):
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(0, "%s: execute begin" % (self.parent.description))
            time.sleep(random.random())     # do "work"
            dprint(0, "%s: execute end" % (self.parent.description))

    def __init__(self):
        FlawFinder.__init__(self, self.__class__.__name__, "FlawFinder Test Class")
    
    # create a no-op action object
    def createAction(self, wiki, pages):
        return self.Action(self, wiki, pages)


# example flaw finder which detects pages whose ID mod 13 == 0
class FFUnlucky(FlawFinder):
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
            
            for i in self.pages:
                if i % 13 == 0: # unlucky ID!
                    with caching.PageIDCache(self.wiki, i) as page:
                        if 'page_title' in page:
                            resultQueue.put(TlgResult(self.wiki, page.values, self.parent))
                
            dprint(3, "%s: execute end" % (self.parent.description))

    def __init__(self):
        FlawFinder.__init__(self, self.__class__.__name__, "Find pages whose ID mod 13 == 0")
    
    def createAction(self, wiki, pages):
        return self.Action(self, wiki, pages)



if __name__ == '__main__':
    FFUnlucky().createAction( 'dewiki_p', [2,4,26] ).execute(Queue.Queue())
    pass

