#!/usr/bin/python
# task list generator - caching utility classes
import os
import re
import sys
import stat
import time
import glob
import json
import threading
import binascii
from utils import *

def MakeTimestamp(unixtime):
    return time.strftime("%Y%m%d%H%M%S", time.gmtime(unixtime))

# base class for cache entries
# cache entries are implemented as context managers, to be used with the "with" statement, see __main__ chunk below.
class FileBasedCache:
    if sys.path[0]=='':     # did pdb eat our path?
        cacheDir= './cache' # use pwd
    else:
        cacheDir= sys.path[0] + '/cache'
    suffix= '.cache'
    tmpSuffix= '.tmp'
    numSubdirs= 256
    hits= 0
    misses= 0
    
    def __init__(self, identifier, expirytimestamp):
        # todo: evaluate if we might ever hit a filename length limit and if so, hash the id with md5 or something.
        self.identifier= identifier
        self.expirytimestamp= expirytimestamp
    
    @staticmethod
    def getSubdirNum(identifier):
        return binascii.crc32(identifier) % FileBasedCache.numSubdirs
    
    @staticmethod
    def getSubdir(identifier):
        return FileBasedCache.cacheDir + '/%02x' % (FileBasedCache.getSubdirNum(identifier))

    @staticmethod
    def initCacheDirs():
        # if the cache dir is not accessible, try to create it
        try:
            dirstat= os.stat(FileBasedCache.cacheDir)
            assert(stat.S_ISDIR(dirstat.st_mode))
        except OSError:
            os.mkdir(FileBasedCache.cacheDir)
        
        # create caching subdirs
        for i in range(0, FileBasedCache.numSubdirs):
            subdir= "%s/%02x" % (FileBasedCache.cacheDir, i)
            try:
                dirstat= os.stat(subdir)
                assert(stat.S_ISDIR(dirstat.st_mode))
            except OSError:
                os.mkdir(subdir)
        
            
    def __enter__(self):        
        # todo: check for identifier-DATE.cache.tmp and wait some time if it exists.

        subdir= self.getSubdir(self.identifier)
        basename= subdir + '/' + self.identifier
        # check if we have a cache entry for this id
        globbed= sorted(glob.glob('%s-??????????????%s' % (basename, FileBasedCache.suffix)))
        now= MakeTimestamp(time.time())
        while len(globbed) and not self.isHit(globbed[0], now):
            fn= globbed.pop(0)
            dprint(3, 'removing %s' % fn)
            os.unlink(fn)
        if len(globbed):
            self.filename= globbed.pop()
        else:
            self.filename= '%s-%s%s' % (basename, self.expirytimestamp, FileBasedCache.suffix)
        
        # if we found a hit, set hit flag and open the file for reading. else, open for writing.
        self.hit= False
        try:
            fstat= os.stat(self.filename)
            assert(stat.S_ISREG(fstat.st_mode))
            self.file= open(self.filename, 'r')
            self.hit= True
            dprint(3, "hit %s!" % self.filename)
            FileBasedCache.hits+= 1
        except OSError:
            # append suffix so other instances won't read the file while we are writing it
            self.file= open(self.filename + FileBasedCache.tmpSuffix, 'w')
            dprint(3, "miss %s!" % self.filename)
            FileBasedCache.misses+= 1
        
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        dprint(3, "FileBasedCache.__exit__")
        if not self.hit:
            # rename the temporary file so it can be found
            os.rename(self.filename + ".tmp", self.filename)
        return False    # if there was an exception, re-raise it
    
    @staticmethod
    def isHit(filename, now):
        match= re.match(".*-([0-9]{14})\.cache", filename)
        expires= match.group(1)
        if expires > now: return True
        else: return False
    
    def write(self, str):
        assert(self.hit==False) # cannot write to a cache entry that comes from disk
        self.file.write(str)

# dict-style cache entry
# constructor reads values from disk
# write access (like "DictCache['foo']=5") writes to disk if the entry is not a cache hit
class DictCache(FileBasedCache):
    def __init__(self, id, lifetime):
        FileBasedCache.__init__(self, id, MakeTimestamp(time.time() + lifetime))

    def __enter__(self):
        FileBasedCache.__enter__(self)
        self.values= dict()
        if self.hit:
            for line in self.file:
                vals= json.loads(line)
                self[vals[0]]= vals[1]
        return self
    
    def __iter__(self):
        return iter(self.values)
    
    def __getitem__(self, key):
        return self.values[key]
    
    def __setitem__(self, key, value):
        self.values[key]= value
        if self.hit == False: 
            dprint(3, "writing %s => %s" % (key, value))
            self.write(json.dumps([key, value]) + "\n")

# list-style iterable cache entry
# constructor reads values from disk
# append() writes to disk if the entry is not a cache hit
class ListCache(FileBasedCache):
    def __init__(self, id, lifetime):
        FileBasedCache.__init__(self, id, MakeTimestamp(time.time() + lifetime))
    
    def __enter__(self):
        FileBasedCache.__enter__(self)
        self.values= list()
        # todo: maybe don't gulp the file here, read line-by-line instead?
        if self.hit:
            for line in self.file:
                vals= json.loads(line)
                self.values.append(vals)
        return self
    
    def __iter__(self):
        return iter(self.values)
    
    def __getitem__(self, index):
        return self.values[index]
    
    def append(self, what):
        self.values.append(what)
        if not self.hit:
            self.write(json.dumps(what) + "\n")

# a cache for a page entry
# behaves like a list of dicts 
# dict entries as described at https://wiki.toolserver.org/view/Database_schema#Page
# TODO is this useful at all, or is it better to use ID-based caches only?
class __PageCache(ListCache):
    def __init__(self, wiki, pageTitle):
        assert(str(pageID)==pageID) # be sure they passed us a string. is this "the" proper way to do this in python?
        ListCache.__init__(self, "page-title-%s-%s" % (wiki, pageTitle), 10*60)
        self.pageTitle= pageTitle
        self.wiki= wiki
    
    def __enter__(self):
        from tlgcatgraph import CatGraphInterface
        ListCache.__enter__(self)
        if not self.hit:
            entry= getPageByTitle(self.wiki, self.pageTitle)
            for e in entry:
                self.append({'page_id': e[0],
                             'page_namespace': e[1],
                             'page_title': e[2],
                             'page_restriction': e[3],
                             'page_counter': e[4],
                             'page_is_redirect': e[5],
                             'page_is_new': e[6],
                             'page_random': e[7],
                             'page_touched': e[8],
                             'page_latest': e[9],
                             'page_len': e[10]})
        return self
    
    def findRowWithFieldValue(self, field, fieldValue):
        for e in self.values:
            if e[field]==fieldValue:
                return e
        return None
    
    def findRowWithNamespace(self, nsID):
        return self.findRowWithFieldValue('page_namespace', nsID)



# a cache for a page entry
# dict entries as described at https://wiki.toolserver.org/view/Database_schema#Page
class PageIDCache(DictCache):
    def __init__(self, wiki, pageID):
        assert(int(pageID)==pageID) # be sure they passed us an int. is this "the" proper way to do this in python?
        DictCache.__init__(self, "page-ID-%s-%s" % (wiki, pageID), 10*60)
        self.pageID= pageID
        self.wiki= wiki
    
    def __enter__(self):
        from tlgcatgraph import CatGraphInterface
        DictCache.__enter__(self)
        if not self.hit:
            entry= getPageByID(self.wiki, self.pageID)
            if len(entry)>0:
                e= entry[0]
                self['page_id']= e[0]
                self['page_namespace']= e[1]
                self['page_title']= e[2]
                self['page_restriction']= e[3]
                self['page_counter']= e[4]
                self['page_is_redirect']= e[5]
                self['page_is_new']= e[6]
                self['page_random']= e[7]
                self['page_touched']= e[8]
                self['page_latest']= e[9]
                self['page_len']= e[10]
        return self


# a cache for a page entry
# dict entries as described at https://wiki.toolserver.org/view/Database_schema#Page
class PageIDFakeCache:
    def __init__(self, wiki, pageID):
        self.pageID= pageID
        self.wiki= wiki
        self.values= dict()
    
    def __enter__(self):
        from tlgcatgraph import CatGraphInterface
        entry= getPageByID(self.wiki, self.pageID)
        if len(entry)>0:
            e= entry[0]
            self['page_id']= e[0]
            self['page_namespace']= e[1]
            self['page_title']= e[2]
            self['page_restriction']= e[3]
            self['page_counter']= e[4]
            self['page_is_redirect']= e[5]
            self['page_is_new']= e[6]
            self['page_random']= e[7]
            self['page_touched']= e[8]
            self['page_latest']= e[9]
            self['page_len']= e[10]
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass
        
    def __iter__(self):
        return iter(self.values)
    
    def __getitem__(self, key):
        return self.values[key]
    
    def __setitem__(self, key, value):
        self.values[key]= value

# initialization
if threading.currentThread().name == 'MainThread':
    FileBasedCache.initCacheDirs()


if __name__ == '__main__':
    with PageIDCache('dewiki_p', getCategoryID('dewiki_p', 'Biologie')) as page:
        print 'cache entry for "Biologie":\n\t', page.values
    
    
    with DictCache('foo-bar-baz', 15) as dcache:
        print "DictCache.hit: %s" % dcache.hit
        if not dcache.hit: 
            dcache['foo']= 7
            dcache["bar"]= "baz"
            dcache["baz"]= (("bar", 5, None), )
            dcache['some_timestamp']= MakeTimestamp(time.time())
        print "values:"
        for i in dcache:  print "\t", i, "=>", dcache[i]
    
    with ListCache("blahblah", 5) as lcache:
        print "ListCache.hit: %s" % lcache.hit
        if not lcache.hit: 
            lcache.append(13)
            lcache.append("foo")
            lcache.append(("bar", 15, None))
            lcache.append(MakeTimestamp(time.time()))
        print "values:"
        for i in lcache:  print "\t", i
