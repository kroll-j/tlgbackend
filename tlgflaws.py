#!/usr/bin/python
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
            grp= klass.shortname.split(':')
            if len(grp)>1: klass.group= grp[0]
            else: klass.group= None
        finally:
            FlawFilters.lock.release()
    

## base class for flaw testers
class FlawFilter:
    def __init__(self, tlg):
        self.tlg= tlg
    
    ## 
    def getPreferredPagesPerAction(self):
        return 1
    
    ## this method should return an object of some class derived from TlgAction
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
    #  @param page a dict containing the full page result page_title, page_id etc.
    def __init__(self, wiki, page, FlawFilter):
        self.wiki= wiki
        self.page= page
        self.FlawFilter= FlawFilter


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
    description= 'Page ID mod 13 == 0. For testing only.'
    
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

FlawFilters.register(FUnlucky)



## 
class FMissingSourcesTemplates(FlawFilter):
    shortname= 'MissingSourcesTemplates'
    description= 'Page has \'missing sources\' templates set.'
    
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
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            cur.execute('SELECT tl_title, tl_from FROM templatelinks WHERE tl_from IN (%s)' % format_strings, self.pageIDs)
            sqlres= cur.fetchall()

            # todo: put this into the sql query.
            for templatelink in sqlres:
                tl_title= templatelink['tl_title']
                try:
                    if tl_title in FMissingSourcesTemplates.templateNamesForWikis[self.wiki]:
                        rows= getPageByID(self.wiki, templatelink['tl_from'])
                        if len(rows):
                            resultQueue.put(TlgResult(self.wiki, rows[0], self.parent))
                except KeyError:
                    # we have no template names for this language version.
                    pass

            
            dprint(3, "%s: execute end" % (self.parent.description))

    def getPreferredPagesPerAction(self):
        return 200

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FMissingSourcesTemplates)


## 
# todo: make results usable by both small + large filter
class FPageSizeBase(FlawFilter):
    
    class Action(TlgAction):
        def execute(self, resultQueue):            
            cur= getCursors()[self.wiki]
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            cur.execute('SELECT page_id, page_len FROM page WHERE page_id IN (%s) AND page_namespace=0 AND page_is_redirect=0' % format_strings, self.pageIDs)
            rows= cur.fetchall()
            try:
                self.parent.pageLengthLock.acquire()
                for row in rows:
                    pagelen= row['page_len']
                    self.parent.pageLengths[row['page_id']]= pagelen
                    self.parent.lengthSum+= pagelen
                self.parent.pagesLeft-= len(self.pageIDs)
            finally:
                self.parent.pageLengthLock.release()

    class FinalAction(TlgAction):
        # todo: the final action stuff takes a while, maybe this can be optimized.
        def execute(self, resultQueue):
            pageLengths= self.parent.pageLengths
            self.avg= self.parent.lengthSum / float(len(pageLengths))
            sum= 0
            for i in pageLengths:
                delta= pageLengths[i]-self.avg
                sum+= delta*delta
            self.stddev= math.sqrt(sum/len(pageLengths))
            dprint(3, "FTPageSize.FinalAction.execute() lengthSum = %d pages = %d avg length = %f stddev = %f" % (self.parent.lengthSum, len(pageLengths), self.avg, self.stddev))

        def canExecute(self):
            return self.parent.pagesLeft == 0
            
    def __init__(self, tlg):
        FlawFilter.__init__(self, tlg)
        self.pagesLeft= len(tlg.getPageIDs())
        self.finalActionCreated= False
        self.pageLengths= {}
        self.pageLengthLock= threading.Lock()
        self.lengthSum= 0
    
    def getPreferredPagesPerAction(self):
        return 50
    
    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))
        if not self.finalActionCreated: 
            actionQueue.put(self.FinalAction(self, language, self.tlg.getPageIDs))
            self.finalActionCreated= True

class FSmall(FPageSizeBase):
    shortname= 'Small'
    description= 'Page is very small, relative to mean page size in result set.'
    
    class FinalAction(FPageSizeBase.FinalAction):
        # todo: the final action stuff takes a while, maybe this can be optimized.
        def execute(self, resultQueue):
            #~ FPageSizeBase.FinalAction.execute(self, resultQueue)
            pageLengths= self.parent.pageLengths
            self.avg= self.parent.lengthSum / float(len(pageLengths))
            threshold= self.avg/4
            for i in pageLengths:
                #~ delta= pageLengths[i]-self.avg
                #~ if delta > self.stddev*8:
                    #~ resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, i)[0], self.parent))
                if pageLengths[i] < threshold:
                    resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, i)[0], self.parent))

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(FPageSizeBase.Action(self, language, pages))
        if not self.finalActionCreated: 
            actionQueue.put(self.FinalAction(self, language, self.tlg.getPageIDs))
            self.finalActionCreated= True

FlawFilters.register(FSmall)


class FLarge(FPageSizeBase):
    shortname= 'Large'
    description= 'Page is very large, relative to mean page size in result set.'
    
    class FinalAction(FPageSizeBase.FinalAction):
        # todo: the final action stuff takes a while, maybe this can be optimized.
        def execute(self, resultQueue):
            FPageSizeBase.FinalAction.execute(self, resultQueue)
            pageLengths= self.parent.pageLengths
            for i in pageLengths:
                delta= pageLengths[i]-self.avg
                if delta > self.stddev*8:
                    resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, i)[0], self.parent))
                #~ if pageLengths[i] < self.avg / 4:
                    #~ resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, i)[0], self.parent))

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(FPageSizeBase.Action(self, language, pages))
        if not self.finalActionCreated: 
            actionQueue.put(self.FinalAction(self, language, self.tlg.getPageIDs))
            self.finalActionCreated= True

FlawFilters.register(FLarge)




## 
class FNoImages(FlawFilter):
    shortname= 'NoImages'
    description= 'Article has no image links.'

    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
                        
            cur= getCursors()[self.wiki]
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            # find all pages in set without any image links
            # the double 'IN' clause seems fishy, maybe i will figure out a better way to do this in sql some time. 
            sqlstr= 'SELECT * FROM page WHERE page_id IN (%s) AND page_namespace=0 AND page_is_redirect=0 AND page_id NOT IN (SELECT il_from FROM imagelinks WHERE il_from IN (%s))' % \
                (format_strings,format_strings)
            dblpages= copy.copy(self.pageIDs)
            dblpages.extend(self.pageIDs)
            cur.execute(sqlstr, dblpages)
            sqlres= cur.fetchall()
            
            for row in sqlres:
                resultQueue.put(TlgResult(self.wiki, row, self.parent))
            
            dprint(3, "%s: execute end" % (self.parent.description))

    def getPreferredPagesPerAction(self):
        return 100

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FNoImages)


# todo: filter for invalid redirects?


if __name__ == '__main__':
    #~ FMissingSourcesTemplates().createActions( 'dewiki_p', [2,4,26] ).execute(Queue.LifoQueue())
    #~ pass
    from tlgbackend import TaskListGenerator
    TaskListGenerator().run('de', 'Fahrzeug -Landfahrzeug -Luftfahrzeug', 4, 'Small')
    #~ stddevtest()
    
