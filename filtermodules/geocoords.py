#!/usr/bin/python
# -*- coding:utf-8 -*-
import time
from tlgflaws import *
from utils import *


## 
class FGeotags(FlawFilter):
    shortname= 'Geotags'
    label= _('Geotags')
    description= _('Get pages with Geotags')
    
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            format_strings = ' OR '.join(['page.page_id=%s'] * len(self.pageIDs))
            cur.execute("""SELECT page.page_id, page.page_namespace, page.page_title, page.page_restrictions, page.page_counter, 
page.page_is_new, page.page_random, page.page_touched, page.page_latest, page.page_len,
geo_tags.gt_lat, geo_tags.gt_lon
FROM page 
JOIN geo_tags ON geo_tags.gt_page_id=page.page_id
WHERE (page.page_namespace=0 OR page.page_namespace=6) AND (%s)""" % format_strings, self.pageIDs)
            res= cur.fetchall()
            
            for row in res:
                filtertitle= '<a target="_blank" href="http://www.openstreetmap.org/?mlat=%s&mlon=%s#map=14/%s/%s">%s,%s</a>' % (row['gt_lat'], row['gt_lon'], row['gt_lat'], row['gt_lon'], row['gt_lat'], row['gt_lon'])
                #~ filtertitle= '%s,%s' % (row['gt_lat'], row['gt_lon'])
                resultQueue.put(TlgResult(self.wiki, row, self.parent, filtertitle))
    
    def getPreferredPagesPerAction(self):
        return 50
    
    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FGeotags)
