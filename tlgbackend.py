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


if __name__ == '__main__':
    import tlgflaws
    
    #s= ('H\xc3\xa4matopoese')
    #s= s.decode('utf-8', errors='replace')
    #s= s.encode('utf-8', errors='replace')
    #print s
    #sys.exit(0)
    
    
    tlg= TaskListGenerator()

    cg= CatGraphInterface(graphname='dewiki')
    pages= set(cg.getPagesInCategory(getCategoryID('dewiki_p', 'Biologie'), 2)) \
            - set(cg.getPagesInCategory(getCategoryID('dewiki_p', 'Meerkatzenverwandte'), 7))   \
            - set(cg.getPagesInCategory(getCategoryID('dewiki_p', 'Astrobiologie'), 7))
    
    print "%d pages" % len(pages)
    sys.stdout.flush()

    flaw= tlgflaws.FFArticleFetchTest()
    for i in pages:
        action= flaw.createAction(i)
        tlg.actionQueue.put(action)
        
    for i in range(0, 1):
        dprint(0, "******** before thread start %d" % i)
        tlg.workerThreads.append(WorkerThread(tlg.actionQueue, tlg.resultQueue))
        #tlg.workerThreads[-1].start()
        tlg.workerThreads[-1].run()
    
    def drainQueue():
        while not tlg.resultQueue.empty():
            foo= tlg.resultQueue.get()
            #foo= foo.decode('utf-8', errors='replace').decode('utf-8', errors='replace')
            print foo

    while threading.activeCount()>1:
        drainQueue()
        time.sleep(0.5)

    for i in tlg.workerThreads:
        i.join()
    
    drainQueue()
    
    from caching import FileBasedCache
    print("cache hits: %d\ncache misses: %d" % (FileBasedCache.hits, FileBasedCache.misses))
    


