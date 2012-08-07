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
        try:
            while True: 
                self.actionQueue.get(True, 0).execute(self.resultQueue)
        except Queue.Empty:
            return

## main app class
class TaskListGenerator:
    def __init__(self):
        self.actionQueue= Queue.Queue()         # actions to process
        self.resultQueue= Queue.Queue()   # results of actions 
        self.mergedResults= {}                    # final merged results, one entry per article
        self.workerThreads= []
        self.cg= None
    
    ## find flaws and print results to output file.
    # @param wiki The wiki graph name ('dewiki', not 'dewiki_p').
    # @param queryString The query string. See CatGraphInterface.executeSearchString documentation.
    # @param queryDepth Search recursion depth.
    # @param flaws String of flaw detector names
    # @param stdout Standard output, a file-like object.
    # @param stderr Standard error, a file-like object.
    def run(self, wiki, queryString, queryDepth, flaws, stdout=sys.stdout, stderr=sys.stderr):
        sys.stdout= stdout
        sys.stderr= stderr
        self.cg= CatGraphInterface(graphname=wiki)
        pageIDs= self.cg.executeSearchString(queryString, queryDepth)
        
        #~ for i in FlawTesters.classInfos:
            #~ klass= FlawTesters.classInfos[i]
            #~ print klass().shortname, "--", klass().description
        
        # create the actions for every article x every flaw
        for flawname in flaws.split():
            try:
                flaw= FlawTesters.classInfos[flawname]()
            except KeyError:
                dprint(0, 'Unknown flaw %s' % flawname)
                return False
            self.createActions(flaw, wiki, pageIDs)
            
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
        sortedResults= sorted(self.mergedResults, key= lambda result: len( self.mergedResults[result]['flaws'] ))
        
        # print results
        for i in sortedResults:
            print json.dumps(self.mergedResults[i])
        
        return True
    
    def createActions(self, flaw, wiki, pageIDs):
        for id in pageIDs:
            action= flaw.createAction( 'dewiki_p', (id,) )
            self.actionQueue.put(action)

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
    def initThreads(self, numThreads=12):
        for i in range(0, numThreads):
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
    TaskListGenerator().run('dewiki', 'Biologie -Meerkatzenverwandte -Astrobiologie', 2, 'Unlucky')
    
    pass
    
    #~ import caching
        
    #~ test().testSingleThread()
    #~ test().testMultiThread(10)
    
    #~ print("cache stats:")
    #~ for i in caching.Stats.__dict__:
        #~ if i[:2] != '__':
            #~ print "%11s: %s" % (i, caching.Stats.__dict__[i])
    


