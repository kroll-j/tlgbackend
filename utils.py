#!/usr/bin/python
# -*- coding:utf-8 -*-
## @file utils.py
import os
import sys
import pwd
import time
import sqlite3
import MySQLdb
import MySQLdb.cursors 
import threading
import getpass
import json
import uuid

from beaker.cache import cache_region, cache_regions

config= { 
    'graphserv-host': 'ortelius',
    'graphserv-port': '6666',
}

DATADIR= '/mnt/user-store/%s/tlgbackend/tip' % getpass.getuser()

beakerCacheDir= os.path.join(DATADIR, 'beaker-cache')

testrun= False

cache_regions.update({
    'mem1h': {          # cache 1 hour in memory, e. g. page ID results
        'expire': 60*60,
        'type': 'memory'
    },
    'disk24h': {        # cache 24h on disk, e. g. category title => ID mappings
        'expire': 60*60*24,
        'type': 'file',
        'data_dir': beakerCacheDir,
    }
})

NS_MAIN = 0
NS_TALK = 1
NS_USER = 2
NS_USER_TALK = 3
NS_PROJECT = 4
NS_PROJECT_TALK = 5
NS_FILE = 6
NS_FILE_TALK = 7
NS_MEDIAWIKI = 8
NS_MEDIAWIKI_TALK = 9
NS_TEMPLATE = 10
NS_TEMPLATE_TALK = 11
NS_HELP = 12
NS_HELP_TALK = 13
NS_CATEGORY = 14
NS_CATEGORY_TALK = 15


class InputValidationError(RuntimeError):
    pass


def GetTempCursors():
    t= threading.currentThread()
    try:
        t.cache
    except AttributeError:
        t.cache= dict()
    try:
        return t.cache['tempcursors']
    except KeyError:
        t.cache['tempcursors']= dict()
        return t.cache['tempcursors']

## a temporary cursor to be used with the 'with' statement. will pe cached per thread and automatically cleaned up.
class TempCursor:
    def __init__(self, host, dbname):
        self.host= host
        self.dbname= dbname
        self.key= '%s.%s' % (host,dbname)
    
    def __enter__(self):
        if self.key in GetTempCursors(): 
            return GetTempCursors()[self.key].cursor
        
        self.conn= None
        while self.conn==None:
            try:
                self.conn= MySQLdb.connect( read_default_file=os.path.expanduser('~')+"/.my.cnf", host=self.host, \
                    use_unicode=False, cursorclass=MySQLdb.cursors.DictCursor )
            except MySQLdb.OperationalError as e:
                if 'max_user_connections' in str(e):
                    dprint(0, 'exceeded max connections, retrying...')
                    time.sleep(0.5)
                    #~ raise   #xxxxx
                else:
                    raise
        
        self.cursor= self.conn.cursor()
        self.cursor.execute ("USE %s" % self.conn.escape_string(self.dbname))
        GetTempCursors()[self.key]= self
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        GetTempCursors()[self.key].lastuse= time.time()


## get cursor for a wikipedia database ('enwiki_p' etc).
#  cursors are created on demand and stored locally for each thread.
#  the DictCursor class is used, i. e. you get dicts with the column names as keys in query results.
def getCursors():
    class Cursors(DictCache):
        def createEntry(self, key):
            if key in getWikiServerMap(): ckey= getWikiServerMap()[key]
            else: ckey= 'sql' # guess
            #~ ckey= getWikiServerMap()[key]
            conn= None
            while conn==None:
                try:
                    conn= getConnections()[ckey]
                except MySQLdb.OperationalError as e:
                    if 'max_user_connections' in str(e):
                        dprint(0, 'exceeded max connections, retrying...')
                        time.sleep(0.5)
                        #~ raise   #xxxxx
                    else:
                        raise
            cur= conn.cursor()
            cur.execute ("USE %s" % conn.escape_string(key))
            return cur
    return CachedThreadValue('SQLCursors', Cursors)

## get page entries matching a given page_title and optional namespace.
#  returns a tuple of dicts containing the result rows, or a tuple with length 0 if not found.
#  @param wiki wiki database name
#  @param pageTitle page title
#  @param pageNamespace namespace to look for, or None (default) for all namespaces
@cache_region('mem1h', 'pageTitles')
def getPageByTitle(wiki, pageTitle, pageNamespace=None):
    cur= getCursors()[wiki]
    query= "SELECT * FROM page WHERE page_title = %s"
    params= [pageTitle]
    if pageNamespace:
        query+= " AND page_namespace = %s"
        params.append(pageNamespace)
    cur.execute(query, params)
    return cur.fetchall()

## get a page entry given its page_id.
#  returns a tuple of dicts containing the result row, or a tuple with length 0 if not found.
@cache_region('mem1h', 'pageIDs')
def getPageByID(wiki, pageID):
    cur= getCursors()[wiki]
    cur.execute("SELECT * FROM page WHERE page_id = %s", (pageID,))
    return cur.fetchall()

## find a category ID given its title.
#  returns category ID, or None if not found.
@cache_region('disk24h')
def getCategoryID(wiki, catTitle):
    page= getPageByTitle(wiki, catTitle, NS_CATEGORY)
    if len(page)!=0:
        return page[0]['page_id']
    else:
        return None

## find everything that links to this page (table 'pagelinks')
@cache_region('mem1h')
def getPagelinksForID(wiki, pageID):
    cur= getCursors()[wiki]
    cur.execute("SELECT * FROM pagelinks WHERE pl_from = %s", (pageID,))
    return cur.fetchall()

## find templates of this page (table 'categorylinks')
@cache_region('mem1h')
def getTemplatelinksForID(wiki, pageID):
    cur= getCursors()[wiki]
    cur.execute("SELECT * FROM templatelinks WHERE tl_from = %s", (pageID,))
    return cur.fetchall()

def MakeTimestamp(unixtime= None):
    if unixtime==None: unixtime= time.time()
    return time.strftime("%Y%m%d %H:%M.%S", time.gmtime(unixtime))

def getUsername():
    return pwd.getpwuid( os.getuid() )[ 0 ]

# logging to files makes filtering hard, logging to sql server is impractical too (what if we want to log an sql connection failure?), so we use sqlite.
def logToDB(timestamp, level, requestID, *args):
    def createLogCursor():
        conn= sqlite3.connect(os.path.join(DATADIR, "log.db"), isolation_level= None, timeout= 30.0)
        logCursor= conn.cursor()
        try:
            logCursor.execute('CREATE TABLE logs(timestamp VARBINARY, level INTEGER, message VARBINARY, requestID CHARACTER)')
            logCursor.execute('CREATE INDEX timestamp ON logs (timestamp)')
            logCursor.execute('CREATE INDEX requestID ON logs (requestID)')
        except sqlite3.OperationalError:
            pass    # table exists
        if threading.currentThread().name == 'MainThread':
            logCursor.execute('DELETE FROM logs WHERE timestamp < ?', (MakeTimestamp(time.time() - 60*60*24*30*3),) )  # remove logs older than 3 months
        return logCursor
    try:
        logCursor= CachedThreadValue('logCursor', createLogCursor)
        logCursor.execute('INSERT INTO logs VALUES (?, ?, ?, ?)', (timestamp, level, unicode(str(*args).decode('utf-8')), requestID))
    except sqlite3.OperationalError:
        pass

# this returns a per-process key for logging.
# make sure this is called at least once in the main thread before any child thread logs anything.
# a unix timestamp is prepended to the ID so that it can be used for sorting by time as well.
__requestID= None
def getRequestID():
    global __requestID
    if __requestID==None:
        __requestID= "%017.4f:%s" % (time.time(), str(uuid.uuid4()))
    return __requestID

debuglevel= 1
## debug print. only shows on stderr if level >= debuglevel. everything is logged to sqlite file DATADIR/log.db.
def dprint(level, *args):
    timestamp= MakeTimestamp()
    
    logToDB(timestamp, level, getRequestID(), *args)
    
    if(debuglevel>=level):
        sys.stderr.write('[%s] ' % timestamp)
        sys.stderr.write(*args)
        sys.stderr.write("\n")

__WikiToServerMapLock= threading.Lock()
# get mapping for wikiname => sql server
# e. g. 'dewiki_p' => 'sql-s5'
def getWikiServerMap():
    global WikiToServerMap
    global __WikiToServerMapLock
    try:
        WikiToServerMap # throws NameError the first time it is executed
    except NameError:
        try:
            __WikiToServerMapLock.acquire() # just in case someone calls this from different threads in parallel.
            def getWikiServerMapping():
                conn= MySQLdb.connect(read_default_file=os.path.expanduser('~')+'/.my.cnf')
                cur= conn.cursor()
                cur.execute("SELECT dbname, server FROM toolserver.wiki WHERE family = 'wikipedia'")
                ret= dict(cur.fetchall())
                for i in ret: ret[i]= 'sql-s%d-rr' % ret[i]
                return ret
            WikiToServerMap= getWikiServerMapping()
        finally:
            __WikiToServerMapLock.release()
    return WikiToServerMap
    
def CachedThreadValue(name, getValue):
    t= threading.currentThread()
    try:
        t.cache
    except AttributeError:
        t.cache= dict()
    try:
        return t.cache[name]
    except KeyError:
        t.cache[name]= getValue()
    return t.cache[name]

class DictCache(dict):
    def __init__(self):
        dict.__init__(self)
    def __missing__(self, key):
        newvalue= self.createEntry(key)
        self.__setitem__(key, newvalue)
        return newvalue
    def createEntry(self, key):
        raise NotImplementedError("createEntry must be reimplemented in subclasses")

def getConnections():
    class Connections(DictCache):
        def createEntry(self, key):
            return MySQLdb.connect( read_default_file=os.path.expanduser('~')+"/.my.cnf", host=key, use_unicode=False, cursorclass=MySQLdb.cursors.DictCursor )
    return CachedThreadValue('SQLConnections', Connections)

if threading.currentThread().name == 'MainThread':
    # precache once in the main thread, not separately for each thread (which would work, but can be slow because of the locking)
    getWikiServerMap()


try:
    filecfg= json.load(file('tlgrc'))
    for key in filecfg:
        dprint(1, "config: %s = %s" % (key, filecfg[key]))
        config[key]= filecfg[key]
except Exception as ex:
    dprint(1, "exception while loading config file tlgrc: %s" % str(ex))


def logStats(statDict):
    global testrun
    if testrun: s= 'testrunstats'
    else: s= 'stats'
    dprint(0, '%s: %s' % (s, json.dumps(statDict)))


if __name__ == '__main__':
    dprint(1, "foo")
    dprint(1, "bar")
    dprint(1, "äöü")
    print getConnections()['sql-s1']
    print getCursors()['dewiki_p']
    print getCursors()
    print getPageByID('dewiki_p', 917280)
    #~ print getPageByTitle('dewiki_p', "Hauptseite")
    #~ print getPageByTitle('dewiki_p', "Biologie", NS_CATEGORY)
    #~ print getCategoryID('dewiki_p', "Biologie")
