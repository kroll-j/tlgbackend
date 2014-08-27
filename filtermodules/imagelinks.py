#!/usr/bin/python
# -*- coding:utf-8 -*-
import time
import itertools
from tlgflaws import *

"""
 ---- Alter Kram ----
licounter= itertools.count()
class FLinkedImages(FlawFilter):
    shortname= 'LinkedImages'                                           # Name, der den Filter identifiziert (nicht übersetzen!)
    label= _('Linked Images')                                           # Label, das im Frontend neben der Checkbox angezeigt wird
    description= _('List all images used by articles in the result set.')    # Längerer Beschreibungstext für Tooltip
    #~ group= _('Timeliness')                      # Gruppe, in die der Filter eingeordnet wird ## XXX welche gruppe passt?

    class Action(TlgAction):
        
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            format_strings = ' OR '.join(['page_id=%s'] * len(self.pageIDs))
            params= []
            params.extend(self.pageIDs)
            
            cur.execute("SELECT imagelinks.il_to, imagelinks.il_from FROM page INNER JOIN imagelinks ON page.page_id=imagelinks.il_from WHERE (%s) AND page.page_namespace=0" 
                % format_strings, params)
            
            titlerows= cur.fetchall()
            
            for row in titlerows:
                #create fake page table entries from image titles
                resultQueue.put(TlgResult(self.wiki, { "page_title": "File:%s" % row['il_to'], 
                            "page_touched": "13372323133742", 
                            "page_counter": 0, 
                            "page_is_redirect": 0, 
                            "page_is_new": 0, 
                            "page_latest": 581426706, 
                            "page_restrictions": "", 
                            "page_len": 4223, 
                            "page_random": 0.0, 
                            "page_id": licounter.next(),      # generate a fake page_id
                            "page_namespace": NS_FILE }, self.parent))

    def getPreferredPagesPerAction(self):
        return 100

    def createActions(self, wiki, pages, actionQueue):
        actionQueue.put(self.Action(self, wiki, pages))

FlawFilters.register(FLinkedImages)
"""

licounter= itertools.count()
class FLinkedFiles_Base(FlawFilter):
    class Action(TlgAction):
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            format_strings= ' OR '.join(['page_id=%s'] * len(self.pageIDs))
            params= []
            params.extend(self.pageIDs)
            
            # find all Files used in the page
            cur.execute("SELECT imagelinks.il_to FROM page INNER JOIN imagelinks ON page.page_id=imagelinks.il_from WHERE (%s) AND page.page_namespace=0" 
                % format_strings, params)
            titlerows= cur.fetchall()
            
            if len(titlerows):
                titles= [ row['il_to'].replace(' ', '_') for row in titlerows ]     # xxx remove?
                format_strings= ' OR '.join(['page_title=%s'] * len(titles))
                params= []
                params.extend(titles)
                cur= getCursors()[self.imagewiki]

                if self.inverted:
                    sqlcmd= 'SELECT page_title FROM page WHERE (%s) and page_namespace=6' % format_strings
                    cur.execute(sqlcmd, params)
                    res= cur.fetchall()
                    diff= set( [ x['il_to'] for x in titlerows ] ).difference( set( [ x['page_title'] for x in res ] ) )
                    #~ dprint(1, str(diff))
                    for title in diff:
                        #create fake page table entries from image titles
                        resultQueue.put(TlgResult(self.wiki, { "page_title": "File:%s" % title, 
                                    "page_touched": "13372323133742", 
                                    "page_counter": 0, 
                                    "page_is_redirect": 0, 
                                    "page_is_new": 0, 
                                    "page_latest": 581426706, 
                                    "page_restrictions": "", 
                                    "page_len": 4223, 
                                    "page_random": 0.0, 
                                    "page_id": title, #licounter.next(),      # generate a fake page_id
                                    "page_namespace": NS_FILE,
                                    'is_fake': True }, self.parent))

                else:
                    sqlcmd= 'SELECT * FROM page WHERE (%s) and page_namespace=6' % format_strings
                    cur.execute(sqlcmd, params)
                    result= cur.fetchall()
                    #~ dprint(1, "result size: %d" % len(result))
                    for page in result:
                        page['page_title']= 'File:%s' % page['page_title']
                        resultQueue.put(TlgResult(self.imagewiki, page, self.parent))

    def getPreferredPagesPerAction(self):
        return 50

    def createActions(self, wiki, pages, actionQueue):
        action= self.Action(self, wiki, pages)
        action.imagewiki= None  # set this to something reasonable in derived classes. (wiki database name such as 'enwiki_p')
        action.inverted= None  # set this to something reasonable in derived classes. (True/False)
        actionQueue.put(action)

class FLinkedFiles_Commons(FLinkedFiles_Base):
    shortname= 'LinkedFiles_Commons'                                           # Name, der den Filter identifiziert (nicht übersetzen!)
    label= _('Linked Files (on Commons)')                                           # Label, das im Frontend neben der Checkbox angezeigt wird
    description= _('List all files used by articles in the result set which are stored in Wikimedia Commons.')    # Längerer Beschreibungstext für Tooltip
    #~ group= _('Timeliness')                      # Gruppe, in die der Filter eingeordnet wird ## XXX welche gruppe passt?

    def createActions(self, wiki, pages, actionQueue):
        action= self.Action(self, wiki, pages)
        action.imagewiki= 'commonswiki_p'
        action.inverted= False
        actionQueue.put(action)

FlawFilters.register(FLinkedFiles_Commons)

class FLinkedFiles_NonCommons(FLinkedFiles_Base):
    shortname= 'LinkedFiles_NonCommons'                                           # Name, der den Filter identifiziert (nicht übersetzen!)
    label= _('Linked Files (not on Commons)')                                           # Label, das im Frontend neben der Checkbox angezeigt wird
    description= _('List all files used by articles in the result set which are not stored in Wikimedia Commons.')    # Längerer Beschreibungstext für Tooltip
    #~ group= _('Timeliness')                      # Gruppe, in die der Filter eingeordnet wird ## XXX welche gruppe passt?

    def createActions(self, wiki, pages, actionQueue):
        action= self.Action(self, wiki, pages)
        action.imagewiki= 'commonswiki_p'
        action.inverted= True
        actionQueue.put(action)

FlawFilters.register(FLinkedFiles_NonCommons)







