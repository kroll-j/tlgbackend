#!/usr/bin/python
# task list generator - backend
import time
import json
import Queue
import threading
import tlgflaws

from tlgcatgraph import CatGraphInterface
from tlgflaws import FlawTesters
from utils import *

## a worker thread which fetches actions from a queue and executes them
class WorkerThread(threading.Thread):
    def __init__(self, actionQueue, resultQueue):
        threading.Thread.__init__(self)
        self.actionQueue= actionQueue
        self.resultQueue= resultQueue
    
    def run(self):
        # todo: create connections here
        try:
            while True: 
                # todo: catch exceptions from execute()
                action= self.actionQueue.get(True, 0)
                # 
                if action.canExecute():
                    action.execute(self.resultQueue)
                else:
                    self.actionQueue.put(action)
        except Queue.Empty:
            return

## main app class
class TaskListGenerator:
    def __init__(self):
        self.actionQueue= Queue.LifoQueue() # actions to process
        self.resultQueue= Queue.LifoQueue() # results of actions 
        self.mergedResults= {}              # final merged results, one entry per article
        self.workerThreads= []
        self.pagesToTest= []                # page IDs to test for flaws
        # todo: check connection limit when several instances of the script are running
        self.numWorkerThreads= 7
        self.wiki= None
        self.cg= None
    
    def listFlaws(self):
        infos= {}
        for i in FlawTesters.classInfos:
            ci= FlawTesters.classInfos[i]
            infos[ci.shortname]= ci.description
        print json.dumps(infos)
    
    ## find flaws and print results to output file.
    # @param wiki The wiki graph name ('dewiki', not 'dewiki_p').
    # @param queryString The query string. See CatGraphInterface.executeSearchString documentation.
    # @param queryDepth Search recursion depth.
    # @param flaws String of flaw detector names
    def run(self, wiki, queryString, queryDepth, flaws):
        self.wiki= wiki
        self.cg= CatGraphInterface(graphname=wiki)
        self.pagesToTest= self.cg.executeSearchString(queryString, queryDepth)
                
        # create the actions for every article x every flaw
        for flawname in flaws.split():
            try:
                flaw= FlawTesters.classInfos[flawname](self)
            except KeyError:
                dprint(0, 'Unknown flaw %s' % flawname)
                return False
            self.createActions(flaw, wiki, self.pagesToTest)
            
        dprint(0, "%d actions to process" % self.actionQueue.qsize())
        
        # spawn the worker threads
        self.initThreads()
        
        # process results as they are created
        while threading.activeCount()>1:
            self.drainResultQueue()
            time.sleep(0.5)
        for i in self.workerThreads:
            i.join()
        # process the last results
        self.drainResultQueue()
        
        # sort by length of flaw list
        sortedResults= sorted(self.mergedResults, key= lambda result: -len( self.mergedResults[result]['flaws'] ))
        
        # print results
        for i in sortedResults:
            print json.dumps(self.mergedResults[i])
        
        return True
    
    ## get IDs of all the pages to be tested for flaws
    def getPageIDs(self):
        return self.pagesToTest
    
    def createActions(self, flaw, wiki, pagesToTest):
        pagesLeft= len(pagesToTest)
        pagesPerAction= min( flaw.getPreferredPagesPerAction(), pagesLeft/self.numWorkerThreads )
        while pagesLeft:
            start= max(0, pagesLeft-pagesPerAction)
            flaw.createActions( self.wiki+'_p', pagesToTest[start:pagesLeft], self.actionQueue )
            pagesLeft-= (pagesLeft-start)
            
    def drainResultQueue(self):
        while not self.resultQueue.empty():
            result= self.resultQueue.get()
            key= '%s:%d' % (result.wiki, result.page['page_id'])
            try:
                # append the name of the flaw to the list of flaws for this article 
                self.mergedResults[key]['flaws'].append(result.flawtester.shortname)
            except KeyError:
                # create a new article in the result set
                self.mergedResults[key]= { 'page': result.page, 'flaws': [result.flawtester.shortname] }

    # create and start worker threads
    def initThreads(self):
        for i in range(0, self.numWorkerThreads):
            self.workerThreads.append(WorkerThread(self.actionQueue, self.resultQueue))
            self.workerThreads[-1].start()




class test:
    def __init__(self):
        self.tlg= TaskListGenerator()

    def createActions(self):
        cg= CatGraphInterface(graphname='dewiki')
        pages= cg.executeSearchString('Biologie -Meerkatzenverwandte -Astrobiologie', 2)
        
        flaw= tlgflaws.FFUnlucky()
        for k in range(0, 3):
            for i in pages:
                action= flaw.createAction( 'dewiki_p', (i,) )
                self.tlg.actionQueue.put(action)
    
    def drainResultQueue(self):
        try:
            while not self.tlg.resultQueue.empty():
                foo= self.tlg.resultQueue.get()
                print foo.encodeAsJSON()
        except (UnicodeEncodeError, UnicodeDecodeError) as exception:  # wtf?!
            raise

    def testSingleThread(self):
        self.createActions()
        numActions= self.tlg.actionQueue.qsize()
        WorkerThread(self.tlg.actionQueue, self.tlg.resultQueue).run()
        self.drainResultQueue()
        print "numActions=%d" % numActions
        sys.stdout.flush()

    def testMultiThread(self, nthreads):
        self.createActions()
        numActions= self.tlg.actionQueue.qsize()
        for i in range(0, nthreads):
            dprint(0, "******** before thread start %d" % i)
            self.tlg.workerThreads.append(WorkerThread(self.tlg.actionQueue, self.tlg.resultQueue))
            self.tlg.workerThreads[-1].start()
        while threading.activeCount()>1:
            self.drainResultQueue()
            time.sleep(0.5)
        for i in self.tlg.workerThreads:
            i.join()
        self.drainResultQueue()
        print "numActions=%d" % numActions
        sys.stdout.flush()


if __name__ == '__main__':
    TaskListGenerator().listFlaws()
    TaskListGenerator().run('dewiki', 'Biologie +Eukaryoten -Rhizarien', 5, 'MissingSourcesTemplates')
    

