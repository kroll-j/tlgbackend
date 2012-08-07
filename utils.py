#!/usr/bin/python
import sys
import MySQLdb
import MySQLdb.cursors 
import threading

from beaker.cache import cache_region, cache_regions

# we use something relative to the script dir for the cache, for now. 
if sys.path[0]=='':                     # did pdb eat our path?
    beakerCacheDir= './beaker-cache'    # use pwd
else:
    beakerCacheDir= sys.path[0] + '/beaker-cache'

cache_regions.update({
    'pages': {          # cache page ID results 1 hour in memory
        'expire': 60*60,
        'type': 'memory'
    },
    'categoryIDs': {    # cache category title => ID mappings 24h on disk
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


# get cursor mapping
# servername => sqlcursor
# cursors are created on demand and local for each thread
# the DictCursor class is used, i. e. you get dicts with the column names as keys in query results
def getCursors():
    class Cursors(DictCache):
        def createEntry(self, key):
            conn= getConnections()[getWikiServerMap()[key]]
            cur= conn.cursor()
            cur.execute ("USE %s" % conn.escape_string(key))
            return cur
    return CachedThreadValue('SQLCursors', Cursors)

# get page entries matching a given page_title and optional namespace
# returns a tuple of dicts containing the result rows, or a tuple with length 0 if not found
@cache_region('pages', 'pageTitles')
def getPageByTitle(wiki, pageTitle, pageNamespace=None):
    cur= getCursors()[wiki]
    query= "SELECT * FROM page WHERE page_title = %s"
    params= [pageTitle]
    if pageNamespace:
        query+= " AND page_namespace = %s"
        params.append(pageNamespace)
    cur.execute(query, params)
    return cur.fetchall()

# get a page entry given its page_id
# returns a tuple of dicts containing the result row, or a tuple with length 0 if not found
@cache_region('pages', 'pageIDs')
def getPageByID(wiki, pageID):
    cur= getCursors()[wiki]
    cur.execute("SELECT * FROM page WHERE page_id = %s", (pageID,))
    return cur.fetchall()

# find a category ID given its title
# returns category ID, or None if not found
@cache_region('categoryIDs')
def getCategoryID(wiki, catTitle):
    page= getPageByTitle(wiki, catTitle, NS_CATEGORY)
    if len(page)!=0:
        return page[0]['page_id']
    else:
        return None


debuglevel= 1
def dprint(level, *args):
    if(debuglevel>=level):
        sys.stdout.write(*args)
        sys.stdout.write("\n")

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
            return MySQLdb.connect( read_default_file="~/.my.cnf", host=key, use_unicode=False, cursorclass=MySQLdb.cursors.DictCursor )
    return CachedThreadValue('SQLConnections', Connections)

if threading.currentThread().name == 'MainThread':
    # precache once in the main thread, not separately for each thread (which would work, but can be slow because of the locking)
    getWikiServerMap()

if __name__ == '__main__':
    print getConnections()['sql-s1']
    print getCursors()['dewiki_p']
    print getCursors()
    print getPageByID('dewiki_p', 917280)
    print getPageByTitle('dewiki_p', "Hauptseite")
    print getPageByTitle('dewiki_p', "Biologie", NS_CATEGORY)
    print getCategoryID('dewiki_p', "Biologie")
