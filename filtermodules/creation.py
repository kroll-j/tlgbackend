#!/usr/bin/python
# -*- coding:utf-8 -*-
import time
from tlgflaws import *

class FSortByCreationTimestamp(FlawFilter):
    shortname= 'CreationTimestamp'                # Name, der den Filter identifiziert (nicht übersetzen!)
    label= _('Creation Timestamp')                # Label, das im Frontend neben der Checkbox angezeigt wird
    description= _('Sort Pages by their creation timestamp.')   # Längerer Beschreibungstext für Tooltip
    group= _('Currentness')                      # Gruppe, in die der Filter eingeordnet wird

    # Die Action-Klasse für diesen Filter
    class Action(TlgAction):
        
        # execute() filtert die Seiten und steckt Ergebnisse in resultQueue.
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            # Formatstrings für mehrere Seiten generieren.
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            # Beginn des heutigen Tages im Format der Wikipedia-Datenbank
            params= []
            params.extend(self.pageIDs)
            
            cur.execute('SELECT page.*,revision.rev_timestamp FROM page INNER JOIN revision ON page_id=rev_page AND rev_parent_id=0 WHERE page_id IN (%s)' % format_strings, params)
            res= cur.fetchall()
            for row in res:
                timestamp= row['rev_timestamp']
                del row['rev_timestamp']
                resultQueue.put(TlgResult(self.wiki, row, self.parent, '%s' % timestamp, sortkey= timestamp))
            
            # Subset der Seiten finden, die heute geändert wurden
            #~ cur.execute('SELECT * FROM page WHERE page_id IN (%s) AND page_touched >= %%s' % format_strings, params)
            #~ changed= cur.fetchall()
            # Alle gefundenen Seiten zurückgeben
            #~ for row in changed:
                #~ resultQueue.put(TlgResult(self.wiki, row, self.parent))

    # Wir wollen 100 Seiten pro Aktion verarbeiten. 
    def getPreferredPagesPerAction(self):
        return 100

    # Eine Aktion erstellen.
    def createActions(self, wiki, pages, actionQueue):
        actionQueue.put(self.Action(self, wiki, pages))

# Beim Laden des Moduls den Filter registrieren:
FlawFilters.register(FSortByCreationTimestamp)


