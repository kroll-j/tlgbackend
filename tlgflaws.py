#!/usr/bin/python
# task list generator - flaws
import os
import sys
import time
import json
import Queue
import random
from utils import *

## flaw tester class information
class FlawTesters:
    classInfos= dict()
    lock= threading.Lock()
    
    @staticmethod
    def register(klass):
        try:
            FlawTesters.lock.acquire()
            FlawTesters.classInfos[klass.shortname]= klass
        finally:
            FlawTesters.lock.release()
    

## base class for flaw testers
class FlawTester:
    def __init__(self, tlg):
        self.tlg= tlg
    
    ## 
    def getPreferredPagesPerAction(self):
        return 1
    
    ## this method should return an object of some class derived from TlgAction
    def createActions(self, wiki, pages, actionQueue):
        raise NotImplementedError("createActions not implemented")


## base class for actions to be executed by task list generator
class TlgAction:
    def __init__(self, parent, wiki, pages):
        self.parent= parent
        self.wiki= wiki
        self.pages= pages
    
    ## test the pages and put TlgResults describing flawed pages into resultQueue 
    def execute(self, resultQueue):
        raise NotImplementedError("execute() not implemented")
    
    ## in subclasses, return False here if this action needs to wait for the result of other actions.
    def canExecute(self):
        return True


## the result of a TlgAction, describing a flawed page
class TlgResult:
    ## constructor
    #  @param page a dict containing the full page result page_title, page_id etc.
    def __init__(self, wiki, page, flawtester):
        self.wiki= wiki
        self.page= page
        self.flawtester= flawtester


## an example flaw tester which does nothing
class FTNop(FlawTester):
    shortname= 'Nop'
    description= 'FlawTester Test Class'
    
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
            time.sleep(0.1) # do "work"
            dprint(3, "%s: execute end" % (self.parent.description))

    # create a no-op action object
    def createActions(self, wiki, pages, actionQueue):
        actionQueue.put(self.Action(self, wiki, pages))

FlawTesters.register(FTNop)


## example flaw tester which detects pages whose ID mod 13 == 0
class FTUnlucky(FlawTester):
    shortname= 'Unlucky'
    description= 'Find pages whose ID mod 13 == 0'
    
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
            
            for i in self.pages:
                if i % 13 == 0: # unlucky ID!
                    rows= getPageByID(self.wiki, i)
                    if len(rows):
                        resultQueue.put(TlgResult(self.wiki, rows[0], self.parent))
            
            dprint(3, "%s: execute end" % (self.parent.description))

    def createActions(self, wiki, pages, actionQueue):
        actionQueue.put(self.Action(self, wiki, pages))

FlawTesters.register(FTUnlucky)



## 
class FTMissingSourcesTemplates(FlawTester):
    shortname= 'MissingSourcesTemplates'
    description= 'Find pages with \'missing sources\' templates'
    
    # store the names of 'missing sources' templates for different language versions.
    # this list is (and probably will always be) incomplete.
    # i know of no centralized list of such template names.
    templateNamesForWikis= {
        'dewiki_p': [ 'Belege_fehlen' ],
        'enwiki_p': [ 'Refimprove' ]
    }
        
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
                        
            cur= getCursors()[self.wiki]
            format_strings = ','.join(['%s'] * len(self.pages))
            cur.execute("SELECT * FROM templatelinks WHERE tl_from IN (%s)" % format_strings, self.pages)
            sqlres= cur.fetchall()

            for templatelink in sqlres:
                tl_title= templatelink['tl_title']
                try:
                    if tl_title in FTMissingSourcesTemplates.templateNamesForWikis[self.wiki]:
                        rows= getPageByID(self.wiki, templatelink['tl_from'])
                        if len(rows):
                            resultQueue.put(TlgResult(self.wiki, rows[0], self.parent))
                except KeyError:
                    # we have no template names for this language version.
                    pass

            
            dprint(3, "%s: execute end" % (self.parent.description))

    def getPreferredPagesPerAction(self):
        return 200

    def createActions(self, wiki, pages, actionQueue):
        actionQueue.put(self.Action(self, wiki, pages))

FlawTesters.register(FTMissingSourcesTemplates)


## 
class FTPageSize(FlawTester):
    shortname= 'PageSize'
    description= 'Find very small/very large pages, relative to mean page size in result set'
    
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
            
            for i in self.pages:
                if i % 13 == 0: # unlucky ID!
                    rows= getPageByID(self.wiki, i)
                    if len(rows):
                        resultQueue.put(TlgResult(self.wiki, rows[0], self.parent))
            
            dprint(3, "%s: execute end" % (self.parent.description))

    def __init__(self, tlg):
        FlawTester.__init__(self, tlg)
        self.pagesLeft= len(tlg.getPages())
    
    def createActions(self, wiki, pages, actionQueue):
        actionQueue.put(self.Action(self, wiki, pages))

FlawTesters.register(FTPageSize)





if __name__ == '__main__':
    #~ FTMissingSourcesTemplates().createActions( 'dewiki_p', [2,4,26] ).execute(Queue.LifoQueue())
    pass
