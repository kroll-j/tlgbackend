#!/usr/bin/python
# -*- coding:utf-8 -*-
# task list generator - flaws
import os
import sys
import time
import math
import copy
import json
import Queue
import random
from utils import *

## flaw tester class information
class FlawFilters:
    classInfos= dict()
    lock= threading.Lock()
    
    @staticmethod
    def register(klass):
        try:
            FlawFilters.lock.acquire()
            FlawFilters.classInfos[klass.shortname]= klass
            if not 'group' in klass.__dict__:
                klass.group= None
            #~ grp= klass.shortname.split(':')
            #~ if len(grp)>1: klass.group= grp[0]
            #~ else: klass.group= None
        finally:
            FlawFilters.lock.release()
    

## base class for flaw filters
class FlawFilter(object):
    def __init__(self, tlg):
        self.tlg= tlg
    
    ## override this method if you want to process more than one article per action.
    def getPreferredPagesPerAction(self):
        return 1
    
    ## this method should return an object of a class derived from TlgAction.
    def createActions(self, language, pages, actionQueue):
        raise NotImplementedError("createActions not implemented")


## base class for actions to be executed by task list generator
class TlgAction:
    def __init__(self, parent, language, pages):
        self.parent= parent
        self.language= language
        self.wiki= language+'wiki_p'
        self.pageIDs= pages
    
    ## test the pages and put TlgResults describing flawed pages into resultQueue 
    def execute(self, resultQueue):
        raise NotImplementedError("execute() not implemented")
    
    ## in subclasses, return False here if this action needs to wait for the result of other actions.
    def canExecute(self):
        return True


## the result of a TlgAction, describing a flawed page
class TlgResult:
    ## constructor
    #  @param wiki the name of the wiki this page was found in
    #  @param page a dict containing the full page result page_title, page_id etc.
    #  @param FlawFilter filter class which found this page
    def __init__(self, wiki, page, FlawFilter, infotext= None, sortkey= 0):
        self.wiki= wiki
        self.page= page
        self.FlawFilter= FlawFilter
        self.infotext= infotext
        self.sortkey= sortkey
        self.marked_as_done= False


## an example flaw tester which does nothing
class FNop(FlawFilter):
    shortname= 'Nop'
    description= 'FlawFilter Test Class'
    
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
            time.sleep(0.1) # do "work"
            dprint(3, "%s: execute end" % (self.parent.description))

    # create a no-op action object
    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

#~ FlawFilters.register(FTNop)


## example flaw tester which detects pages whose ID mod 13 == 0
class FUnlucky(FlawFilter):
    shortname= 'Unlucky'
    label= 'Unlucky'
    description= 'Page ID mod 13 == 0. For testing only.'
    group= 'Test'
    
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
            
            for i in self.pageIDs:
                if i % 13 == 0: # unlucky ID!
                    rows= getPageByID(self.wiki, i)
                    if len(rows):
                        resultQueue.put(TlgResult(self.wiki, rows[0], self.parent))
            
            dprint(3, "%s: execute end" % (self.parent.description))

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

#~ FlawFilters.register(FUnlucky)


if __name__ == '__main__':
    import gettext
    gettext.translation('tlgbackend', localedir= os.path.join(sys.path[0], 'messages'), languages=['de']).install()
    
    from tlgbackend import TaskListGenerator
    TaskListGenerator().run('de', 'Fahrzeug; -Landfahrzeug; -Luftfahrzeug', 4, 'TemplateMissingSources')
    #~ stddevtest()
    
