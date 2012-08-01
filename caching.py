#!/usr/bin/python
# task list generator - caching utility classes
import os
import re
import sys
import stat
import time
import glob
import json

debuglevel= 1
def dprint(level, *args):
    if(debuglevel>=level):
        sys.stdout.write(*args)
        sys.stdout.write("\n")

def MakeTimestamp(unixtime):
    return time.strftime("%Y%m%d%H%M%S", time.gmtime(unixtime))

# base class for cache entries
class FileBasedCache:
    cacheDir= sys.path[0] + '/cache'
    
    def __init__(self, identifier, expirytimestamp):
        self.identifier= identifier
        
        # if the cache dir is not accessible, try to create it
        try:
            dirstat= os.stat(FileBasedCache.cacheDir)
            assert(stat.S_ISDIR(dirstat.st_mode))
        except OSError:
            os.mkdir(FileBasedCache.cacheDir)

        # check if we have a cache entry for this id
        globbed= sorted(glob.glob('%s/%s-??????????????.cache' % (FileBasedCache.cacheDir, identifier)))
        now= MakeTimestamp(time.time())
        while len(globbed) and not self.isHit(globbed[0], now):
            fn= globbed.pop(0)
            dprint(3, 'removing %s' % fn)
            os.unlink(fn)
        if len(globbed):
            self.filename= globbed.pop()
        else:
            self.filename= '%s/%s-%s.cache' % (FileBasedCache.cacheDir, identifier, expirytimestamp)
        self.hit= False
        try:
            fstat= os.stat(self.filename)
            assert(stat.S_ISREG(fstat.st_mode))
            self.file= open(self.filename, 'r')
            self.hit= True
        except OSError:
            self.file= open(self.filename, 'w')
    
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
class DictCache(FileBasedCache, dict):
    def __init__(self, id, lifetime):
        FileBasedCache.__init__(self, id, MakeTimestamp(time.time() + lifetime))
        dict.__init__(self)
        self.values= dict()
        if self.hit:
            for line in self.file:
                vals= json.loads(line)
                self[vals[0]]= vals[1]
    
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
        self.values= list()
        if self.hit:
            for line in self.file:
                vals= json.loads(line)
                self.values.append(vals)
        
    def __iter__(self):
        return iter(self.values)
    
    def __getitem__(self, index):
        return self.values[index]
    
    def append(self, what):
        self.values.append(what)
        if not self.hit:
            self.write(json.dumps(what) + "\n")


if __name__ == '__main__':
    dcache= DictCache('foo-bar-baz', 5)
    print "dcache.hit: %s" % dcache.hit
    if not dcache.hit: 
        dcache['foo']= 7
        dcache["bar"]= "baz"
        dcache["baz"]= (("bar", 5, None), )
        dcache['some_timestamp']= MakeTimestamp(time.time())
    print "values:"
    for i in dcache:  print "\t", i, "=>", dcache[i]
    
    lcache= ListCache("blahblah", 5)
    print "lcache.hit: %s" % lcache.hit
    if not lcache.hit: 
        lcache.append(13)
        lcache.append("foo")
        lcache.append(("bar", 5, None))
        lcache.append(MakeTimestamp(time.time()))
    print "values:"
    for i in lcache:  print "\t", i

