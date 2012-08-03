#!/usr/bin/python
# task list generator - backend
import time
import Queue
import threading
import tlgflaws

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






class test:
    def __init__(self):
        self.tlg= TaskListGenerator()

    def createActions(self):
        cg= CatGraphInterface(graphname='dewiki')
        pages= cg.executeSearchString('Biologie -Meerkatzenverwandte -Astrobiologie', 2)
        
        flaw= tlgflaws.FFArticleFetchTest()
        for k in range(0, 3):
            for i in pages:
                action= flaw.createAction( 'dewiki_p', (i,) )
                self.tlg.actionQueue.put(action)
    
    def drainQueue(self):
        try:
            while not self.tlg.resultQueue.empty():
                foo= self.tlg.resultQueue.get()
                print(foo)
        except (UnicodeEncodeError, UnicodeDecodeError) as exception:  # wtf?!
            raise

    def testSingleThread(self):
        self.createActions()
        numActions= self.tlg.actionQueue.qsize()
        WorkerThread(self.tlg.actionQueue, self.tlg.resultQueue).run()
        self.drainQueue()
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
            self.drainQueue()
            time.sleep(0.5)
        for i in self.tlg.workerThreads:
            i.join()
        self.drainQueue()
        print "numActions=%d" % numActions
        sys.stdout.flush()


if __name__ == '__main__':
    import caching
        
    test().testSingleThread()
    #~ test().testMultiThread(10)
    
    print("cache stats:")
    for i in caching.Stats.__dict__:
        if i[:2] != '__':
            print "%11s: %s" % (i, caching.Stats.__dict__[i])
    


