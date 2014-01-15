#!/usr/bin/python
# -*- coding:utf-8 -*-
import time
import itertools
from tlgflaws import *


licounter= itertools.count()
## Ein Filter, der alle Seiten findet, die heute geändert wurden.
class FLinkedImages(FlawFilter):
    shortname= 'LinkedImages'                                           # Name, der den Filter identifiziert (nicht übersetzen!)
    label= _('Linked Images')                                           # Label, das im Frontend neben der Checkbox angezeigt wird
    description= _('List all images used by articles in the result set.')    # Längerer Beschreibungstext für Tooltip
    #~ group= _('Timeliness')                      # Gruppe, in die der Filter eingeordnet wird

    # Die Action-Klasse für diesen Filter
    class Action(TlgAction):
        
        # execute() filtert die Seiten und steckt Ergebnisse in resultQueue.
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            # Formatstrings für mehrere Seiten generieren.
            #~ format_strings = ','.join(['%s'] * len(self.pageIDs))
            format_strings = ' OR '.join(['page_id=%s'] * len(self.pageIDs))
            params= []
            params.extend(self.pageIDs)
            
            cur.execute("SELECT imagelinks.il_to, imagelinks.il_from FROM page INNER JOIN imagelinks ON page.page_id=imagelinks.il_from WHERE (%s) AND page.page_namespace=0" 
                % format_strings, params)
            
            titlerows= cur.fetchall()
            
            #create fake page table entries from image titles
            for row in titlerows:
                #~ fakepage= 
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
            
            
            
            
            #~ # Subset der Seiten finden, die heute geändert wurden
            #~ cur.execute('SELECT * FROM page WHERE page_id IN (%s) AND page_touched >= %%s' % format_strings, params)
            #~ changed= cur.fetchall()
            #~ # Alle gefundenen Seiten zurückgeben
            #~ for row in changed:
                #~ resultQueue.put(TlgResult(self.wiki, row, self.parent))

    # Wir wollen 100 Seiten pro Aktion verarbeiten. 
    def getPreferredPagesPerAction(self):
        return 100

    # Eine Aktion erstellen.
    def createActions(self, wiki, pages, actionQueue):
        actionQueue.put(self.Action(self, wiki, pages))

# Beim Laden des Moduls den Filter registrieren:
FlawFilters.register(FLinkedImages)







