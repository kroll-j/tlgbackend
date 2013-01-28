#!/usr/bin/python
# -*- coding:utf-8 -*-
# task list generator - backend
import os
import sys
import gettext
import time
import json
import Queue
import traceback
import threading
import tlgflaws
import wiki

from tlgcatgraph import CatGraphInterface
from tlgflaws import FlawFilters
from utils import *

# todo: daemon threads?

## a worker thread which fetches actions from a queue and executes them
class WorkerThread(threading.Thread):
    def __init__(self, actionQueue, resultQueue, wikiname, runEvent):
        threading.Thread.__init__(self)
        self.actionQueue= actionQueue
        self.resultQueue= resultQueue
        self.wikiname= wikiname
        self.daemon= True
        self.runEvent= runEvent
        self.currentAction= ''
    
    def setCurrentAction(self, infoString):
        if infoString.split(':'): self.currentAction= infoString.split(':')[-1]
        else: self.currentAction= infoString
    def getCurrentAction(self):
        return self.currentAction
    
    def run(self):
        try:
            # create cursor which will most likely be used by actions later
            cur= getCursors()[self.wikiname+'_p']
            # wait until action queue is ready
            self.runEvent.wait()
            
            try:
                while True: 
                    # todo: if there are only actions left which cannot be run, exit the thread
                    action= self.actionQueue.get(True, 0)
                    if action.canExecute():
                        self.setCurrentAction(action.parent.shortname)
                        action.execute(self.resultQueue)
                    else:
                        dprint(3, "re-queueing action " + str(action) + " from %s, queue len=%d" % (action.parent.shortname, self.actionQueue.qsize()))
                        self.actionQueue.put(action)
                    
                    tempCursors= GetTempCursors()
                    rmkeys= []
                    for key in tempCursors:
                        tc= tempCursors[key]
                        if time.time() - tc.lastuse > 3:
                            dprint(0, 'closing temp cursor %s' % tc.key)
                            tc.cursor.close()
                            tc.conn.close()
                            rmkeys.append(key)
                    for k in rmkeys:
                        del tempCursors[k]
                            
            except Queue.Empty:
                self.setCurrentAction('')
                tempCursors= GetTempCursors()
                for key in tempCursors:
                    tc= tempCursors[key]
                    tc.cursor.close()
                    tc.conn.close()
                tempCursors.clear()
                # todo: close other open connections
                return
        
        except Exception:
            # unhandled exception, propagate to main thread
            self.resultQueue.put(sys.exc_info())


# replacing Queue with this lock-free container might be faster
import collections
class QueueWrapper(collections.deque):
    def init(self):
        collections.deque.__init__(self)

    def get(self, block=True, timeout=None):    # block and timeout are ignored
        try:
            return self.popleft()
        except IndexError:
            raise Queue.Empty()
    
    def put(self, item):
        self.append(item)
        
    def qsize(self):
        return len(self)
    
    def empty(self):
        return len(self)==0

        
## main app class
class TaskListGenerator:
    def __init__(self):
        self.actionQueue= QueueWrapper()    #Queue.Queue()     # actions to process
        self.resultQueue= QueueWrapper()    #Queue.Queue()     # results of actions 
        self.mergedResults= {}              # final merged results, one entry per article
        self.workerThreads= []
        self.pagesToTest= []                # page IDs to test for flaws
        self.numWorkerThreads= 10
        self.language= None                 # language code e.g. 'en'
        self.wiki= None                     # e.g. 'enwiki'
        self.cg= None
        self.runEvent= threading.Event()
        self.loadFilterModules()
        self.simpleMW= None # SimpleMW instance
    
    def getActiveWorkerCount(self):
        count= 0
        for t in self.workerThreads:
            if t.isAlive(): count+= 1
        return count
    
    @staticmethod
    def mkStatus(string):
        status= json.dumps({'status': string})
        return status
        
    def loadFilterModules(self):
        import imp
        for root, dirs, files in os.walk(os.path.join(sys.path[0], 'filtermodules')):
            for name in files:
                if name[-3:]=='.py':
                    file= None
                    module= None
                    try:
                        modname= name[:-3]
                        (file, pathname, description)= imp.find_module(modname, [root])
                        module= imp.load_module(modname, file, pathname, description)
                    except Exception as e:
                        dprint(0, "error occured while loading filter module %s, exception string was '%s'" % (modname, str(e)))
                        pass
                    finally:
                        if file: file.close()
                        if module: dprint(3, "loaded filter module '%s'" % modname)

    def getFlawList(self):
        infoString= '{\n'
        firstLine= True
        for i in sorted(FlawFilters.classInfos):
            ci= FlawFilters.classInfos[i]
            if not firstLine:
                infoString+= ',\n'
            firstLine= False
            infoString+= '\t"%s": %s' % (ci.shortname, json.dumps({ 'group': ci.group, 'label': ci.label, 'description': ci.description }))
        infoString+= '\n}\n'
        return infoString
    
    def listFlaws(self):
        print self.getFlawList()
    
    ## evaluate a single query category.
    # 'wl:USER,TOKEN' special syntax queries USER's watchlist instead of CatGraph.
    def evalQueryCategory(self, string, defaultdepth):
        separatorChar= '#'  # special separator char for things like 'title#PAGETITLE'
        s= string.split(separatorChar, 1)
        if len(s)==1:
            res= self.cg.getPagesInCategory(string.replace(' ', '_'), defaultdepth)
            #~ print res
            return res
        else:
            if s[0]=='wl':  # watchlist
                wlparams= s[1].split(',')
                if len(wlparams)!=2:
                    raise InputValidationError(_('Watchlist syntax is: wl%cUSERNAME,TOKEN') % separatorChar)
                res= []
                for pageid in self.simpleMW.getWatchlistPages(wlparams[0], wlparams[1]):
                    res.append(pageid)
                return res
            elif s[0]=='title': # single page
                if len(s)!=2:
                    raise InputValidationError(_('Use: \'title%cPAGETITLE\'') % separatorChar)
                row= getPageByTitle(self.wiki + '_p', s[1], 0)
                if len(row)==0:
                    raise InputValidationError(_('Page not found in mainspace: %s') % s[1])
                return (row[0]['page_id'], )
            else:
                raise InputValidationError(_('invalid query type: \'%s\'') % s[0])
    
    def evalQueryString(self, string, depth):
        result= set()
        n= 0
        for param in string.split(';'):
            param= param.strip()
            if len(param)==0:
                raise InputValidationError(_('Empty category name specified.'))
            if param[0] in '+-':
                category= param[1:].strip()
                op= param[0]
            else:
                category= param
                op= '|'
            if op=='|':
                result|= set(self.evalQueryCategory(category, depth))
                dprint(2, ' | "%s"' % category)
            elif op=='+':
                if n==0:
                    # '+' on first category should do the expected thing
                    result|= set(self.evalQueryCategory(category, depth))
                    dprint(2, ' | "%s"' % category)
                else:
                    result&= set(self.evalQueryCategory(category, depth))
                    dprint(2, ' & "%s"' % category)
            elif op=='-':
                # '-' on first category has no effect
                if n!=0:
                    result-= set(self.evalQueryCategory(category, depth))
                    dprint(2, ' - "%s"' % category)
            n+= 1
        return list(result)
    
    
    ## find flaws (generator function).
    # @param lang The wiki language code ('de', 'fr').
    # @param queryString The query string. See CatGraphInterface.executeSearchString documentation.
    # @param queryDepth Search recursion depth.
    # @param flaws String of flaw detector names
    def generateQuery(self, lang, queryString, queryDepth, flaws):
        try:
            begin= time.time()
            
            self.language= lang
            self.wiki= lang + 'wiki'
            self.simpleMW= wiki.SimpleMW(lang)

            dprint(0, 'generateQuery(): lang "%s", query string "%s", depth %s, flaws "%s"' % (lang, queryString, queryDepth, flaws))
            
            # spawn the worker threads
            self.initThreads()
            
            if len(queryString)==0:
                # todo: use InputValidationError exception
                yield '{"exception": "%s"}' % _('Empty category search string.')
                return
            
            yield self.mkStatus(_('evaluating query string \'%s\' with depth %d') % (queryString, int(queryDepth)))

            self.cg= CatGraphInterface(host=config['graphserv-host'], port=int(config['graphserv-port']), graphname=self.wiki)
            #~ self.pagesToTest= self.cg.executeSearchString(queryString, queryDepth)
            self.pagesToTest= self.evalQueryString(queryString, queryDepth)
            
            yield self.mkStatus(_('query found %d results.') % len(self.pagesToTest))

            # todo: add something like MaxWaitTime, instead of this
            #~ if len(self.pagesToTest) > 50000:
                #~ raise RuntimeError('result set of %d pages is too large to process in a reasonable time, please modify your search string.' % len(self.pagesToTest))
            
            # create the actions for every page x every flaw
            for flawname in flaws.split():
                try:
                    flaw= FlawFilters.classInfos[flawname](self)
                except KeyError:
                    raise InputValidationError('Unknown flaw %s' % flawname)
                self.createActions(flaw, self.language, self.pagesToTest)
            
            numActions= self.actionQueue.qsize()
            yield self.mkStatus(_('%d pages to test, %d actions to process') % (len(self.pagesToTest), numActions))
            
            # signal worker threads that they can run
            self.runEvent.set()
            
            # process results as they are created
            actionsProcessed= 0 #numActions-self.actionQueue.qsize()
            while self.getActiveWorkerCount()>0:
                self.drainResultQueue()
                n= max(numActions-self.actionQueue.qsize()-(self.getActiveWorkerCount()), 0)
                if n!=actionsProcessed:
                    actionsProcessed= n
                    eta= (time.time()-begin) / actionsProcessed * (numActions-actionsProcessed)
                    yield json.dumps( { 'progress': '%d/%d' % (actionsProcessed, numActions) } )
                    #~ yield self.mkStatus(_('%d of %d actions processed (eta: %02d:%02d)') % (actionsProcessed, numActions, int(eta)/60, int(eta)%60))
                    yield self.mkStatus(_('%d of %d actions processed') % (actionsProcessed, numActions))
                time.sleep(0.25)
            for i in self.workerThreads:
                i.join()
            # process the last results
            self.drainResultQueue()
            
            #~ # sort by length of flaw list, flaw list, and page title
            #~ sortedResults= sorted(self.mergedResults, key= lambda result: \
                #~ (-len(self.mergedResults[result]['flaws']), sorted(self.mergedResults[result]['flaws']), self.mergedResults[result]['page']['page_title']))
            
            # sort
            sortedResults= sorted(self.mergedResults, key= lambda i: \
                (-len(self.mergedResults[i]),                                                           # length of flaw list, 
                 sorted( map(lambda x: x.FlawFilter.shortname, self.mergedResults[i]) ),                # flaw list (alphabetical), 
                 map( lambda x: x[1], sorted( map(lambda x: (x.FlawFilter.shortname, x.sortkey), 
                         self.mergedResults[i]), 
                         key= lambda x: x[1]) ),  # sort key,  
                 self.mergedResults[i][0].page['page_title']))                                          # page title (alphabetical)
            
            yield self.mkStatus(_('%d pages tested in %d actions. %d pages in result set. processing took %.1f seconds. please wait while the result list is being transferred.') % \
                (len(self.pagesToTest), numActions, len(self.mergedResults), time.time()-begin))
            
            dprint(1, '%d pages tested in %d actions. %d pages in result set. processing took %.1f seconds.' % \
                (len(self.pagesToTest), numActions, len(self.mergedResults), time.time()-begin))
            
            # print results
            for i in sortedResults:
                result= self.mergedResults[i]
                d= { 'page': result[0].page,         #['page_title'].replace('_', ' '), 
                     'flaws': map( lambda res: { 'name': res.FlawFilter.shortname, 'infotext': res.infotext, 'done': False }, result )
                    }
                yield json.dumps(d)
        
        except InputValidationError as e:
            yield '{"exception": "%s:\\n%s"}' % (_('Input validation failed'), str(e))
        
        except Exception as e:
            info= sys.exc_info()
            dprint(0, traceback.format_exc(info[2]))
            yield '{"exception": "%s"}' % (traceback.format_exc(info[2]).replace('\n', '\\n').replace('"', '\\"'))
            return
    
    ## get IDs of all the pages to be tested for flaws
    def getPageIDs(self):
        return self.pagesToTest
    
    def createActions(self, flaw, language, pagesToTest):
        pagesLeft= len(pagesToTest)
        pagesPerAction= max(1, min( flaw.getPreferredPagesPerAction(), pagesLeft/self.numWorkerThreads ))
        while pagesLeft:
            start= max(0, pagesLeft-pagesPerAction)
            flaw.createActions( self.language, pagesToTest[start:pagesLeft], self.actionQueue )
            pagesLeft-= (pagesLeft-start)
            
    
    def processResultOld(self, result):
        key= '%s:%d' % (result.wiki, result.page['page_id'])
        try:
            # append the name of the flaw to the list of flaws for this article 
            #~ self.mergedResults[key]['flaws'].append(result.FlawFilter.shortname)
            self.mergedResults[key]['flaws'].append(result.filtertitle)
            self.mergedResults[key]['flaws'].sort()
        except KeyError:
            # create a new article in the result set
            self.mergedResults[key]= { 'page': result.page, 'flaws': [result.filtertitle] }
    
    def processResult(self, result):
        from getpass import getuser

        # todo: maybe cache results and check for 'done' marks every N results
        marked= False
        with TempCursor('sql', 'u_%s_tlgbackend_u' % getuser()) as cursor:
            cursor.execute("SELECT * FROM marked_as_done WHERE filter_name = %s AND page_latest = '%s'", (result.FlawFilter.shortname, result.page['page_latest']))
            if cursor.fetchone()!=None:
                marked= True
        
        if marked: return   # maybe optionally return result with a special marker, later
        
        key= '%s:%d' % (result.wiki, result.page['page_id'])
        try:
            self.mergedResults[key].append(result)
            self.mergedResults[key].sort(key= lambda x: x.FlawFilter.shortname)
        except KeyError:
            self.mergedResults[key]= [ result ]
    
    def processWorkerException(self, exc_info):
        raise exc_info[0], exc_info[1], exc_info[2] # re-throw exception from worker thread
        
    def drainResultQueue(self):
        while not self.resultQueue.empty():
            result= self.resultQueue.get()
            if isinstance(result, tlgflaws.TlgResult): self.processResult(result)
            else: self.processWorkerException(result)

    # create and start worker threads
    def initThreads(self):
        for i in range(0, self.numWorkerThreads):
            self.workerThreads.append(WorkerThread(self.actionQueue, self.resultQueue, self.wiki, self.runEvent))
            self.workerThreads[-1].start()

    def markAsDone(self, pageID, pageTitle, pageRev, filterName, unmark):
        from getpass import getuser
        import MySQLdb
        dbname= 'p_%s_tlgbackend' % getuser()
        tablename= 'marked_as_done'
        conn= MySQLdb.connect(read_default_file=os.path.expanduser('~')+"/.my.cnf", host='sql', use_unicode=False, cursorclass=MySQLdb.cursors.DictCursor)
        cursor= conn.cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS %s' % conn.escape_string(dbname))
        cursor.execute('USE %s' % conn.escape_string(dbname))
        cursor.execute("""CREATE TABLE IF NOT EXISTS %s (
            page_id INT(10) UNSIGNED,
            page_title VARBINARY(255),
            page_latest INT(10) UNSIGNED,
            filter_name VARBINARY(255),
            UNIQUE KEY (page_latest, filter_name),
            KEY (page_id),
            KEY (page_title),
            KEY (page_latest),
            KEY (filter_name))""" % tablename)
        if unmark:
            cursor.execute('DELETE FROM ' + tablename + " WHERE page_latest = %s AND filter_name = %s", 
                (pageRev, filterName))
        else:
            cursor.execute('REPLACE INTO ' + tablename + ' VALUES (%s, %s, %s, %s)', 
                (pageID, pageTitle, pageRev, filterName))
        conn.commit()
        cursor.close()
        conn.close()


class test:
    def __init__(self):
        self.tlg= TaskListGenerator()

    def createActions(self):
        cg= CatGraphInterface(graphname='dewiki')
        pages= cg.executeSearchString('Biologie -Meerkatzenverwandte -Astrobiologie', 2)
        
        flaw= tlgflaws.FFUnlucky()
        for k in range(0, 3):
            for i in pages:
                action= flaw.createAction( 'de', (i,) )
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
    gettext.translation('tlgbackend', localedir= os.path.join(sys.path[0], 'messages'), languages=['de']).install()
    #~ TaskListGenerator().listFlaws()
    #~ TaskListGenerator().run('de', 'Biologie +Eukaryoten -Rhizarien', 5, 'PageSize')
    #~ for line in TaskListGenerator().generateQuery('de', 'Biologie; +Eukaryoten; -Rhizarien', 4, 'NoImages'):
    for line in TaskListGenerator().generateQuery('fr', 'Plante fruitière', 2, 'ALL'):
    #~ for line in TaskListGenerator().generateQuery('de', 'Politik; +Physik', 3, 'ALL'):
    #~ for line in TaskListGenerator().generateQuery('de', '+wl:Johannes Kroll (WMDE),xxxxx', 3, 'ALL'):
        print line
        sys.stdout.flush()
    

