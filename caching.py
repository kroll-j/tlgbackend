#!/usr/bin/python
# task list generator - caching utility classes
import os
import json

CACHE_DIR= sys.path[0] + '/cache/'

class FileBasedCache:
    def __init__(self, identifier):
        
        self.filename= 'cache/%s.cache' % identifier
        try:
            self.file= open(self.filename, 'r')
            self.hit= True
        except IOError:
            self.hit= False
            self.file= open(self.filename, 'w')
    
    def write(str):
        assert(self.hit==False)
        self.file.write(str)
    

class ResultCache(FileBasedCache):
    pass
