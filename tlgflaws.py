#!/usr/bin/python
# task list generator - flaws
import os
import sys
import time
import Queue
import random
import caching
import tlgbackend
from utils import *

# base class for actions to be executed by task list generator
class TlgAction:
    def __init__(self, parent, wiki, pages):
        self.parent= parent
        self.wiki= wiki
        self.pages= pages
    
    def execute(self, resultQueue):
        raise NotImplementedError("execute() not implemented")

# base class for flaw finders
class FlawFinder:
    def __init__(self, shortname, description):
        self.shortname= shortname
        self.description= description
    
    # this method should return an object of some class derived from TlgAction
    def createAction(self, wiki, pages):
        raise NotImplementedError("execute() not implemented")

# an example of a flaw finder which does nothing
class FFTest(FlawFinder):
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(0, "%s: execute begin" % (self.parent.description))
            time.sleep(random.random())     # do "work"
            dprint(0, "%s: execute end" % (self.parent.description))

    def __init__(self):
        FlawFinder.__init__(self, "FFTest", "FlawFinder Test Class")
    
    # create a no-op action object
    def createAction(self, wiki, pages):
        return self.Action(self, wiki, pages)


class FFArticleFetchTest(FlawFinder):
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
            
            for i in self.pages:
                with caching.PageIDCache('dewiki_p', i) as page:
                    if 'page_title' in page:
                        #print page['page_title']
                        resultQueue.put(page['page_title'])
                
            dprint(3, "%s: execute end" % (self.parent.description))

    def __init__(self):
        FlawFinder.__init__(self, "FFArticleFetchTest", "Article Fetch Test")
    
    # create a no-op action object
    def createAction(self, wiki, pages):
        return self.Action(self, wiki, pages)


# 
class FFIDByThirteen(FlawFinder):
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
            
            for i in self.pages:
                with caching.PageIDCache('dewiki_p', i) as page:
                    if 'page_title' in page:
                        #print page['page_title']
                        resultQueue.put(page['page_title'])
                
            dprint(3, "%s: execute end" % (self.parent.description))

    def __init__(self):
        FlawFinder.__init__(self, "FFArticleFetchTest", "Article Fetch Test")
    
    # create a no-op action object
    def createAction(self, wiki, pages):
        return self.Action(self, wiki, pages)



if __name__ == '__main__':
    FFArticleFetchTest().createAction( 'dewiki_p', [2,4] ).execute(Queue.Queue())
    pass

