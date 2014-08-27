#!/usr/bin/python
# -*- coding:utf-8 -*-
import time
import datetime
import requests
from tlgflaws import *
from utils import *

# snippet from http://stackoverflow.com/questions/1265665/python-check-if-a-string-represents-an-int-without-using-try-except ...
def isInt_str(v):
    v = str(v).strip()
    return v=='0' or (v if v.find('..') > -1 else v.lstrip('-+').rstrip('0').rstrip('.')).isdigit()

## 
class FPagehits(FlawFilter):
    shortname= 'Pagehits'
    label= _('Page Hits')
    description= _('Sort articles by hit count. Uses data from stats.grok.se from previous month.')

    @staticmethod
    def makeGrokSession():
        grokSession= requests.Session()
        # if they ever need to complain about our requests, they'll know where to look:
        grokSession.headers.update({ 'User-Agent': 'Article List Generator (http://tools.wmflabs.org/render/stools/alg'})
        return grokSession
    
    @cache_region('disklongterm')
    def getHitcount(self, year, month, title):
        try:
            # the requests library is totally thread-safe, except when it isn't. 
            # so we need to work around by creating a separate session for each worker thread
            session= CachedThreadValue("grokSession", self.makeGrokSession)
            res= session.get('http://stats.grok.se/json/de/%s%02d/%s' % (year, int(month), title))
            if res.status_code==200:
                json= res.json()
                total= 0
                for day in json['daily_views']:
                    total+= int(json['daily_views'][day])
                return total
        except Exception as ex:
            return str(ex)  # ....
        return '?'
    
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            format_strings = ' OR '.join(['page_id=%s'] * len(self.pageIDs))
            cur.execute("""SELECT page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect, 
page_is_new, page_random, page_touched, page_latest, page_len 
FROM page WHERE (page_namespace=0 OR page_namespace=6) AND page_is_redirect=0 AND (%s)""" % format_strings, self.pageIDs)
            res= cur.fetchall()
            
            lastmonth= datetime.datetime.fromtimestamp(time.time())
            statyear= lastmonth.year
            statmonth= lastmonth.month
            
            for row in res:
                count= self.parent.getHitcount(statyear, statmonth, row['page_title'])
                filtertitle= 'count: %s' % count
                sortkey= -int(count) if isInt_str(count) else 1
                resultQueue.put(TlgResult(self.wiki, row, self.parent, filtertitle, sortkey= sortkey))
    
    def getPreferredPagesPerAction(self):
        return 50
    
    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FPagehits)
