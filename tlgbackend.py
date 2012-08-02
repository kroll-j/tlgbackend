#!/usr/bin/python
# task list generator - backend
import time
import Queue
import threading
from tlgcatgraph import CatGraphInterface
from utils import *

# a worker thread which fetches actions from a queue and executes them
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

# main app class
class TaskListGenerator:
    def __init__(self):
        self.actionQueue= Queue.Queue()
        self.resultQueue= Queue.Queue()
        self.workerThreads= []
    
    #def run(self):





import tlgflaws

def testSingleThread():
    tlg= TaskListGenerator()

    cg= CatGraphInterface(graphname='dewiki')
    pages= set(cg.getPagesInCategory(getCategoryID('dewiki_p', 'Biologie'), 2)) \
            - set(cg.getPagesInCategory(getCategoryID('dewiki_p', 'Meerkatzenverwandte'), 7))   \
            - set(cg.getPagesInCategory(getCategoryID('dewiki_p', 'Astrobiologie'), 7))
    
    flaw= tlgflaws.FFArticleFetchTest()
    for k in range(0, 3):
        for i in pages:
            action= flaw.createAction(i)
            tlg.actionQueue.put(action)
        
    print "%d actions" % tlg.actionQueue.qsize()
    sys.stdout.flush()

    WorkerThread(tlg.actionQueue, tlg.resultQueue).run()
    
    try:
        while not tlg.resultQueue.empty():
            foo= tlg.resultQueue.get()
            print foo   #.decode('utf-8', errors='replace')
    except UnicodeEncodeError, UnicodeDecodeError:
        print " ************** ", foo
        raise

def testMultiThread(nthreads):
    tlg= TaskListGenerator()

    cg= CatGraphInterface(graphname='dewiki')
    pages= set(cg.getPagesInCategory(getCategoryID('dewiki_p', 'Biologie'), 2)) \
            - set(cg.getPagesInCategory(getCategoryID('dewiki_p', 'Meerkatzenverwandte'), 7))   \
            - set(cg.getPagesInCategory(getCategoryID('dewiki_p', 'Astrobiologie'), 7))
    
    flaw= tlgflaws.FFArticleFetchTest()
    for k in range(0, 3):
        for i in pages:
            action= flaw.createAction(i)
            tlg.actionQueue.put(action)

    print "%d actions" % tlg.actionQueue.qsize()
    sys.stdout.flush()
        
    for i in range(0, nthreads):
        dprint(0, "******** before thread start %d" % i)
        tlg.workerThreads.append(WorkerThread(tlg.actionQueue, tlg.resultQueue))
        tlg.workerThreads[-1].start()
        #~ tlg.workerThreads[-1].run()
    
    def drainQueue():
        try:
            while not tlg.resultQueue.empty():
                foo= tlg.resultQueue.get()
                print(foo)
        except UnicodeEncodeError, UnicodeDecodeError:
            print " ************** ", foo.decode('utf-8', errors='replace')
            raise

    while threading.activeCount()>1:
        drainQueue()
        time.sleep(0.5)

    for i in tlg.workerThreads:
        i.join()
    
    drainQueue()



if __name__ == '__main__':
    import caching
    
    caching.PageIDCache= caching.PageIDMemCache
    
    #~ testSingleThread()
    testMultiThread(10)
    
    print("cache stats:")
    for i in caching.Stats.__dict__:
        if i[:2] != '__':
            print "%11s: %s" % (i, caching.Stats.__dict__[i])

    sys.exit(1)
    


