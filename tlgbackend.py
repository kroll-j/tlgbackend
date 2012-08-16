#!/usr/bin/python
# task list generator - backend
import time
import json
import Queue
import threading
import tlgflaws

from tlgcatgraph import CatGraphInterface
from tlgflaws import FlawFilters
from utils import *

# todo: check what happens when uncaught exceptions get thrown (script doesn't exit)
# daemon threads

## a worker thread which fetches actions from a queue and executes them
class WorkerThread(threading.Thread):
    def __init__(self, actionQueue, resultQueue, wikiname, runEvent):
        threading.Thread.__init__(self)
        self.actionQueue= actionQueue
        self.resultQueue= resultQueue
        self.wikiname= wikiname
        self.daemon= True
        self.runEvent= runEvent
    
    def run(self):
        # create cursor which will most likely be used by actions later
        cur= getCursors()[self.wikiname+'_p']
        # wait until action queue is ready
        self.runEvent.wait()
        try:
            while True: 
                # todo: catch exceptions from execute()
                # todo: if there are only actions left which cannot be run, exit the thread
                action= self.actionQueue.get(True, 0)
                # 
                if action.canExecute():
                    action.execute(self.resultQueue)
                else:
                    dprint(3, "re-queueing action " + str(action) + " from %s, queue len=%d" % (action.parent.shortname, self.actionQueue.qsize()))
                    self.actionQueue.put(action)
        except Queue.Empty:
            return

class ResultSetTooLargeException(Exception):
    pass

## main app class
class TaskListGenerator:
    def __init__(self):
        self.actionQueue= Queue.Queue()     # actions to process
        self.resultQueue= Queue.Queue()     # results of actions 
        self.mergedResults= {}              # final merged results, one entry per article
        self.workerThreads= []
        self.pagesToTest= []                # page IDs to test for flaws
        # todo: check connection limit when several instances of the script are running
        self.numWorkerThreads= 7
        self.wiki= None
        self.cg= None
        self.runEvent= threading.Event()
        self.loadFilterModules()
    
    @staticmethod
    def mkStatus(string):
        return json.dumps({'status': string})
        
    # wip, testing
    def loadFilterModules(self):
        import imp
        for root, dirs, files in os.walk(os.path.join(sys.path[0], 'filtermodules')):
            for name in files:
                if name[-3:]=='.py':
                    #~ self.printStatus('importing ' + os.path.join(root, name))
                    file= None
                    module= None
                    try:
                        modname= name[:-3]
                        (file, pathname, description)= imp.find_module(modname, [root])
                        module= imp.load_module(modname, file, pathname, description)
                    except Exception as e:
                        #~ self.printStatus("error occured while loading filter module %s, exception string was '%s'" % (modname, str(e)))
                        pass
                    finally:
                        if file: file.close()
                        #~ if module: self.printStatus("loaded filter module '%s'" % modname)
    
    def listFlaws(self):
        infos= {}
        for i in FlawFilters.classInfos:
            ci= FlawFilters.classInfos[i]
            infos[ci.shortname]= ci.description
        print json.dumps(infos)
    
    ## find flaws and print results to output file.
    # @param lang The wiki language code ('de', 'fr').
    # @param queryString The query string. See CatGraphInterface.executeSearchString documentation.
    # @param queryDepth Search recursion depth.
    # @param flaws String of flaw detector names
    def run(self, lang, queryString, queryDepth, flaws):
        self.wiki= lang + 'wiki'

        # spawn the worker threads
        self.initThreads()
        
        self.cg= CatGraphInterface(graphname=self.wiki)
        self.pagesToTest= self.cg.executeSearchString(queryString, queryDepth)

        # todo: add something like MaxWaitTime, instead of this
        if len(self.pagesToTest) > 50000:
            raise RuntimeError('result set of %d pages is too large to process in a reasonable time, please modify your search string.' % len(self.pagesToTest))
        
        # create the actions for every page x every flaw
        for flawname in flaws.split():
            try:
                flaw= FlawFilters.classInfos[flawname](self)
            except KeyError:
                raise RuntimeError('Unknown flaw %s' % flawname)
            self.createActions(flaw, self.wiki, self.pagesToTest)
            
        numActions= self.actionQueue.qsize()
        dprint(0, "%d pages to test, %d actions to process" % (len(self.pagesToTest), numActions))
        
        # signal worker threads that they can run
        self.runEvent.set()
        
        # process results as they are created
        actionsProcessed= numActions-self.actionQueue.qsize()
        while threading.activeCount()>1:
            self.drainResultQueue()
            n= numActions-self.actionQueue.qsize()
            if n!=actionsProcessed:
                actionsProcessed= n
                dprint(0, "%d/%d actions processed" % (actionsProcessed, numActions))
            time.sleep(0.25)
        for i in self.workerThreads:
            i.join()
        # process the last results
        self.drainResultQueue()
        
        # sort by length of flaw list, flaw list, and page title
        sortedResults= sorted(self.mergedResults, key= lambda result: \
            (-len(self.mergedResults[result]['flaws']), sorted(self.mergedResults[result]['flaws']), self.mergedResults[result]['page']['page_title']))
        
        # print results
        for i in sortedResults:
            print json.dumps(self.mergedResults[i])
        
        return True
    
    # testing generator stuff
    def generateQuery(self, lang, queryString, queryDepth, flaws):
        yield self.mkStatus("foo string, bar, etc")
        
        self.wiki= lang + 'wiki'

        # spawn the worker threads
        self.initThreads()
        
        self.cg= CatGraphInterface(graphname=self.wiki)
        self.pagesToTest= self.cg.executeSearchString(queryString, queryDepth)
        
        yield self.mkStatus("after executeSearchString")

        # todo: add something like MaxWaitTime, instead of this
        if len(self.pagesToTest) > 50000:
            raise RuntimeError('result set of %d pages is too large to process in a reasonable time, please modify your search string.' % len(self.pagesToTest))
        
        # create the actions for every page x every flaw
        for flawname in flaws.split():
            try:
                flaw= FlawFilters.classInfos[flawname](self)
            except KeyError:
                raise RuntimeError('Unknown flaw %s' % flawname)
            self.createActions(flaw, self.wiki, self.pagesToTest)
            
        numActions= self.actionQueue.qsize()
        yield self.mkStatus("%d pages to test, %d actions to process" % (len(self.pagesToTest), numActions))
        
        # signal worker threads that they can run
        self.runEvent.set()
        
        # process results as they are created
        actionsProcessed= numActions-self.actionQueue.qsize()
        while threading.activeCount()>1:
            self.drainResultQueue()
            n= numActions-self.actionQueue.qsize()
            if n!=actionsProcessed:
                actionsProcessed= n
                yield self.mkStatus("%d/%d actions processed" % (actionsProcessed, numActions))
            time.sleep(0.25)
        for i in self.workerThreads:
            i.join()
        # process the last results
        self.drainResultQueue()
        
        # sort by length of flaw list, flaw list, and page title
        sortedResults= sorted(self.mergedResults, key= lambda result: \
            (-len(self.mergedResults[result]['flaws']), sorted(self.mergedResults[result]['flaws']), self.mergedResults[result]['page']['page_title']))
        
        # print results
        for i in sortedResults:
            yield json.dumps(self.mergedResults[i])
        
        return
    
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
                self.mergedResults[key]['flaws'].append(result.FlawFilter.shortname)
            except KeyError:
                # create a new article in the result set
                self.mergedResults[key]= { 'page': result.page, 'flaws': [result.FlawFilter.shortname] }

    # create and start worker threads
    def initThreads(self):
        for i in range(0, self.numWorkerThreads):
            self.workerThreads.append(WorkerThread(self.actionQueue, self.resultQueue, self.wiki, self.runEvent))
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
    #~ TaskListGenerator().listFlaws()
    #~ TaskListGenerator().run('de', 'Biologie +Eukaryoten -Rhizarien', 5, 'PageSize')
    for line in TaskListGenerator().generateQuery('de', 'Biologie +Eukaryoten -Rhizarien', 5, 'PageSize'):
        print line
        sys.stdout.flush()
    

