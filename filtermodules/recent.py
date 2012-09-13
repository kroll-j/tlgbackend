#!/usr/bin/python
# -*- coding:utf-8 -*-
import time
from tlgflaws import *

## Ein Filter, der alle Seiten findet, die heute geändert wurden.
class FRecentlyChanged(FlawFilter):
    shortname= 'RecentlyChanged'                # Name, der den Filter identifiziert (nicht übersetzen!)
    label= _('Recently Changed')                # Label, das im Frontend neben der Checkbox angezeigt wird
    description= _('Page was touched today.')   # Längerer Beschreibungstext für Tooltip
    group= _('Timeliness')                      # Gruppe, in die der Filter eingeordnet wird

    # Die Action-Klasse für diesen Filter
    class Action(TlgAction):
        
        # execute() filtert die Seiten und steckt Ergebnisse in resultQueue.
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            # Formatstrings für mehrere Seiten generieren.
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            # Beginn des heutigen Tages im Format der Wikipedia-Datenbank
            today= time.strftime( '%Y%m%d000000', time.localtime(time.time()) )
            params= []
            params.extend(self.pageIDs)
            params.append(today)
            # Subset der Seiten finden, die heute geändert wurden
            cur.execute('SELECT * FROM page WHERE page_id IN (%s) AND page_touched >= %%s' % format_strings, params)
            changed= cur.fetchall()
            # Alle gefundenen Seiten zurückgeben
            for row in changed:
                resultQueue.put(TlgResult(self.wiki, row, self.parent))

    # Wir wollen 100 Seiten pro Aktion verarbeiten. 
    def getPreferredPagesPerAction(self):
        return 100

    # Eine Aktion erstellen.
    def createActions(self, wiki, pages, actionQueue):
        actionQueue.put(self.Action(self, wiki, pages))

# Beim Laden des Moduls den Filter registrieren:
#FlawFilters.register(FRecentlyChanged)







