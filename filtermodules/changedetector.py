#!/usr/bin/python
import time
from tlgflaws import *

## this filter finds articles listed in the ChangeDetector database.
class FChangeDetector(FlawFilter):
    shortname= 'ChangeDetector'
    label= 'Change Detector'
    # todo: insert changedetector link?
    description= _('Page seems to be outdated compared to the same article in other Wikipedia language versions (Change Detector data).')
    group= _('Currentness')

    class Action(TlgAction):
        def execute(self, resultQueue):
            if TOOLSERVER:
                host= 'sql'
                db= 'p_render_change_detector_p'
            else:
                host= 'tools-db'
                db= 'p50380g50454__change_detector'
            with TempCursor(host, db) as cur:
                format_strings = ','.join(['%s'] * len(self.pageIDs))
                date= time.strftime( '%Y%m%d', time.localtime(time.time()-60*60*24) )
                params= []
                params.extend(self.pageIDs)
                params.append(self.language)
                params.append(date)
                # get the changedetector identifiers of all pages which were NOT changed that day
                cur.execute('SELECT page_id,identifier FROM noticed_article WHERE page_id IN (%s) AND language = %%s AND day = %%s AND detected_by_cta=0 AND detected_by_cts=0 AND detected_by_mdf=0' % format_strings, params)
                unchanged= cur.fetchall()
                if len(unchanged):
                    # for each unchanged page, check whether the page was changed in other languages on that day.
                    for row in unchanged:
                        #~ dprint(1, 'unchanged: %s' % str(row))
                        cur.execute('SELECT identifier,language FROM changed_article WHERE identifier=%s AND day=%s AND only_major!=0 AND non_bot!=0 AND many_user!=0 GROUP BY language', (row['identifier'], date))
                        res= cur.fetchall()
                        if len(res) > 5:    # xxx this value depends on the setting in change.ini 
                            #~ dprint(1, 'ident: %s' % str(res))
                            # fmt= ', '.join(['%s'] * len(res))
                            # info= fmt % map(lambda x: x['language'], res) # "not enough arguments for format string", why?!
                            info= ''
                            for s in map(lambda x: x['language'], res):
                                info+= ' '
                                info+= s
                            resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, row['page_id'])[0], self.parent, infotext= 'changed in: %s' % info))

    def getPreferredPagesPerAction(self):
        return 100

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FChangeDetector)







