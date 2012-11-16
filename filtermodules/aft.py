#!/usr/bin/python
# -*- coding:utf-8 -*-
import time
from tlgflaws import *
from utils import *

## 
class FAFT(FlawFilter):
    shortname= 'ArticleFeedback'
    label= _('Article Feedback')
    description= _('Less than 60% of readers found what they are looking for, according to Article Feedback Tool v5.')
    
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            cur.execute('SELECT * FROM aft_article_feedback_ratings_rollup WHERE arr_page_id IN (%s) AND arr_field_id=16 AND arr_count!=0' % format_strings, self.pageIDs)
            aftResult= cur.fetchall()
            pageIDs= []
            for row in aftResult:
                foundRatio= row['arr_total'] / row['arr_count']
                if foundRatio < 0.6:
                    pageIDs.append(row['arr_page_id'])
            
            if len(pageIDs):
                format_strings = ','.join(['%s'] * len(pageIDs))
                cur.execute('SELECT * FROM page WHERE page_id IN (%s) AND page_namespace=0' % format_strings, pageIDs)
                for row in cur.fetchall():
                    resultQueue.put(TlgResult(self.wiki, row, self.parent))
            
    
    def getPreferredPagesPerAction(self):
        return 100
    
    @cache_region('disk24h')
    def hasAFTData(self, language):
        cur= getCursors()[language + 'wiki_p']
        try:
            cur.execute('DESCRIBE aft_article_feedback_ratings_rollup')
        except MySQLdb.ProgrammingError as ex:
            return False
        return True

    def createActions(self, language, pages, actionQueue):
        if not self.hasAFTData(language):
            raise InputValidationError(_('Article Feedback data is not yet available for the selected language.'))
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FAFT)
