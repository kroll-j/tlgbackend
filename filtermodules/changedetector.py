#!/usr/bin/python
import time
from tlgflaws import *

## this filter finds articles listed in the ChangeDetector database.
class FChangeDetector(FlawFilter):
    shortname= 'Timeliness:ChangeDetector' # the name indicates that this filter belongs to group Currentness.
    # todo: insert changedetector link?
    description= 'The article seems to be outdated compared to the same article in other Wikipedia language versions (ChangeDetector data).'

    class Action(TlgAction):
        def execute(self, resultQueue):
            dprint(3, "%s: execute begin" % (self.parent.description))
            
            cur= getCursors()['p_render_change_detector_p']
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            date= time.strftime( '%Y%m%d', time.localtime(time.time()-60*60*24) )
            params= []
            params.extend(self.pageIDs)
            params.append(date)
            # get the changedetector identifiers of all pages which were NOT changed that day
            cur.execute('SELECT page_id,identifier FROM noticed_article WHERE page_id IN (%s) AND day = %%s AND detected_by_cta=0 AND detected_by_cts=0 AND detected_by_mdf=0' % format_strings, params)
            unchanged= cur.fetchall()
            if len(unchanged):
                # for each unchanged page, check whether the page was changed in other languages on that day.
                for row in unchanged:
                    cur.execute('SELECT identifier FROM changed_article WHERE identifier = %s AND day = %s GROUP BY language', (row['identifier'], date))
                    res= cur.fetchall()
                    if len(res) > 4:    # xxx this value depends on the setting in change.ini 
                        resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, row['page_id'])[0], self.parent))
            
            dprint(3, "%s: execute end" % (self.parent.description))

    def getPreferredPagesPerAction(self):
        return 100

    def createActions(self, wiki, pages, actionQueue):
        actionQueue.put(self.Action(self, wiki, pages))

FlawFilters.register(FChangeDetector)







