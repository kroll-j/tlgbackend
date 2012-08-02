#!/usr/bin/python
import sys
import MySQLdb
import threading


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


debuglevel= 1
def dprint(level, *args):
    if(debuglevel>=level):
        sys.stdout.write(*args)
        sys.stdout.write("\n")

__WikiToServerMapLock= threading.Lock()
def getWikiServerMap():
    global WikiToServerMap
    global __WikiToServerMapLock
    try:
        WikiToServerMap # throws NameError the first time it is executed
    except NameError:
        try:
            __WikiToServerMapLock.acquire() # just in case someone calls this from different threads in parallel.
            def getWikiServerMapping():
                dprint(2, "getWikiServerMapping called")
                conn= MySQLdb.connect(read_default_file='~/.my.cnf')
                cur= conn.cursor()
                cur.execute("SELECT dbname, server FROM toolserver.wiki WHERE family = 'wikipedia'")
                ret= dict(cur.fetchall())
                for i in ret: ret[i]= 'sql-s%d' % ret[i]
                return ret
            WikiToServerMap= getWikiServerMapping()
        finally:
            __WikiToServerMapLock.release()
    return WikiToServerMap
    
def CachedThreadValue(name, getValue):
    t= threading.currentThread()
    try:
        return t.__dict__[name]
    except KeyError:
        t.__dict__[name]= getValue()
    return t.__dict__[name]

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
            return MySQLdb.connect( read_default_file="~/.my.cnf", host=key, use_unicode=False )
    return CachedThreadValue('SQLConnections', Connections)

def getCursors():
    class Cursors(DictCache):
        def createEntry(self, key):
            conn= getConnections()[getWikiServerMap()[key]]
            cur= conn.cursor()
            cur.execute ("USE %s" % conn.escape_string(key))
            return cur
    return CachedThreadValue('SQLCursors', Cursors)

def getPageByTitle(wiki, pageTitle):
    cur= getCursors()[wiki]
    cur.execute("SELECT * FROM page WHERE page_title = %s", (pageTitle,))
    return cur.fetchall()

def getPageByID(wiki, pageID):
    cur= getCursors()[wiki]
    cur.execute("SELECT * FROM page WHERE page_id = %s", (pageID,))
    return cur.fetchall()

def getCategoryID(wiki, catTitle):
    cur= getCursors()[wiki]
    cur.execute("SELECT * FROM page WHERE page_title = %s AND page_namespace = %s", (catTitle, NS_CATEGORY))
    return cur.fetchall()[0][0]

if threading.currentThread().name == 'MainThread':
    # precache once in the main thread, not separately for each thread (which would work, but can be slow because of the locking)
    getWikiServerMap()

if __name__ == '__main__':
    #print getWikiServerMap()['dewiki_p']
    #~ print getCursors()
    #~ print getCursors()
    print getConnections()['sql-s1']
    print getCursors()['dewiki_p']
    print getCursors()
    print getPageByID('dewiki_p', 917280)
