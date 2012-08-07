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
            FlawTesters.classInfos[klass().shortname]= klass
        finally:
            FlawTesters.lock.release()
    

## base class for flaw testers
class FlawTester:
    def __init__(self, shortname, description):
        self.shortname= shortname
        self.description= description
    
    # this method should return an object of some class derived from TlgAction
    def createAction(self, wiki, pages):
        raise NotImplementedError("execute() not implemented")


## base class for actions to be executed by task list generator
class TlgAction:
    def __init__(self, parent, wiki, pages):
        self.parent= parent
        self.wiki= wiki
        self.pages= pages
    
    def execute(self, resultQueue):
        raise NotImplementedError("execute() not implemented")


## the result of a TlgAction, describing a flawed article
class TlgResult:
    # page: a dict containing the full page result page_title, page_id etc.
    def __init__(self, wiki, page, flawtester):
        self.wiki= wiki
        self.page= page
        self.flawtester= flawtester
    
    def encodeAsJSON(self):
        return json.dumps( { 'wiki': self.wiki, 'page': self.page, 'found-by': self.FlawTester } )


## an example flaw tester which does nothing
class FTNop(FlawTester):
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
            time.sleep(0.1)     #(random.random()*.1+.1)     # do "work"
            dprint(3, "%s: execute end" % (self.parent.description))

    def __init__(self):
        FlawTester.__init__(self, 'Nop', 'FlawTester Test Class')
    
    # create a no-op action object
    def createAction(self, wiki, pages):
        return self.Action(self, wiki, pages)

FlawTesters.register(FTNop)


## example flaw tester which detects pages whose ID mod 13 == 0
class FTUnlucky(FlawTester):
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

    def __init__(self):
        FlawTester.__init__(self, 'Unlucky', 'Find pages whose ID mod 13 == 0')
    
    def createAction(self, wiki, pages):
        return self.Action(self, wiki, pages)

FlawTesters.register(FTUnlucky)



## 
class FTMissingSourcesTemplates(FlawTester):
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
            
            for i in self.pages:
                templatelinks= getTemplatelinksForID(self.wiki, i)
                for template in templatelinks:
                    tl_title= template['tl_title']
                    try:
                        if tl_title in FTMissingSourcesTemplates.templateNamesForWikis[self.wiki]:
                            rows= getPageByID(self.wiki, i)
                            if len(rows):
                                resultQueue.put(TlgResult(self.wiki, rows[0], self.parent))
                    except KeyError:
                        # we have no template names for this language version.
                        pass
            
            dprint(3, "%s: execute end" % (self.parent.description))

    def __init__(self):
        FlawTester.__init__(self, 'MissingSourcesTemplates', 'Find pages with \'missing sources\' templates')
    
    def createAction(self, wiki, pages):
        return self.Action(self, wiki, pages)

FlawTesters.register(FTMissingSourcesTemplates)



if __name__ == '__main__':
    FTUnlucky().createAction( 'dewiki_p', [2,4,26] ).execute(Queue.Queue())
    pass

