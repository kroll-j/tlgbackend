#!/usr/bin/python
# -*- coding:utf-8 -*-
import time
from tlgflaws import *

## 
class FAll(FlawFilter):
    shortname= 'ALL'
    label= _('All Pages')
    description= _('Show all articles without filtering.')
    
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            #~ format_strings = ','.join(['%s'] * len(self.pageIDs))
            #~ cur.execute('SELECT * FROM page WHERE page_id IN (%s)' % format_strings, self.pageIDs)
            format_strings = ' OR '.join(['page_id=%s'] * len(self.pageIDs))
            cur.execute("""SELECT page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect, 
page_is_new, page_random, page_touched, page_latest, page_len 
FROM page WHERE (page_namespace=0 OR page_namespace=6) AND page_is_redirect=0 AND (%s)""" % format_strings, self.pageIDs)
            result= cur.fetchall()
            for row in result:
                resultQueue.put(TlgResult(self.wiki, row, self.parent))
    
    def getPreferredPagesPerAction(self):
        return 500

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FAll)

## 
class FAllCategories(FlawFilter):
    shortname= 'ALLCAT'
    label= _('All Categories')
    description= _('Show all categories without filtering.')
    
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            format_strings = ' OR '.join(['page_id=%s'] * len(self.pageIDs))
            cur.execute("""SELECT page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect, 
page_is_new, page_random, page_touched, page_latest, page_len 
FROM page WHERE page_namespace=14 AND page_is_redirect=0 AND (%s)""" % format_strings, self.pageIDs)
            result= cur.fetchall()
            for row in result:
                resultQueue.put(TlgResult(self.wiki, row, self.parent))
    
    def getPreferredPagesPerAction(self):
        return 500

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FAllCategories)

##  base class for filters which check for lists of templates.
# todo: add entries for more languages (?)
class FTemplatesBase(FlawFilter):
    def __init__(self, tlg, templateNames):
        FlawFilter.__init__(self, tlg)
        self.templateNamesForWikis= templateNames
        #~ for wikidb in templateNames:
            #~ for template in templateNames[wikidb]:
                #~ dprint(0, "%s %s: %s" % (wikidb, template, getCategoryID(wikidb, 'Wikipedia:'+template)))
    
    class Action(TlgAction):
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            cur.execute('SELECT tl_title, tl_from FROM templatelinks WHERE tl_from IN (%s)' % format_strings, self.pageIDs)
            sqlres= cur.fetchall()

            # todo: put this into the sql query.
            for templatelink in sqlres:
                tl_title= templatelink['tl_title']
                try:
                    if tl_title in self.parent.templateNamesForWikis[self.wiki]:
                        rows= getPageByID(self.wiki, templatelink['tl_from'])
                        if len(rows):
                            resultQueue.put(TlgResult(self.wiki, rows[0], self.parent))
                except KeyError:
                    # we have no template names for this language version.
                    pass

    def getPreferredPagesPerAction(self):
        return 200

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))


## create a class that filters for templates
#  @param templateNames dict of iterables containing lists of templates to search for. dict key is wiki db name.
def makeTemplateFilter(shortname, label, description, group, templateNames):
    def init(self, tlg):
        FTemplatesBase.__init__(self, tlg, templateNames)
    return type('F'+shortname, (FTemplatesBase,), {'__init__': init, 'shortname': shortname, 'label': label, 'description': description, 'group': group})

def registerTemplateFilter(*args):
    FlawFilters.register(makeTemplateFilter(*args))

registerTemplateFilter('TemplateNeutrality', _('Template: Neutrality'), _('Page has \'neutrality\' template set.'), _('Neutrality'), {
    'dewiki_p': [ 'Neutralität' ],
    'enwiki_p': [ 'Neutrality' ],
    'frwiki_p': [ 'Désaccord_de_neutralité' ],
    'cswiki_p': [ 'NPOV' ],
})

registerTemplateFilter('TemplateMissingSources', _('Template: Refimprove'), _('Page has \'missing sources\' template set.'), _('Completeness'), {
    'dewiki_p': [ 'Belege_fehlen' ],
    'enwiki_p': [ 'Refimprove' ],
    'frwiki_p': [ 'À_sourcer' ],
})

registerTemplateFilter('TemplateObsolete', _('Template: Out of date'), _('Page has \'out of date\' template set.'), _('Currentness'), {
    'dewiki_p': [ 'Veraltet' ],
    'enwiki_p': [ 'Out_of_date' ],
    'frwiki_p': [ 'Mettre_à_jour' ],
    'cswiki_p': [ 'Aktualizovat' ],
})

registerTemplateFilter('TemplateCleanup', _('Template: Cleanup'), _('Page has \'cleanup\' template set.'), None, {
    'dewiki_p': [ 'Überarbeiten' ],
    'enwiki_p': [ 'Cleanup' ],
    'frwiki_p': [ 'À_recycler' ],
    'cswiki_p': [ 'Upravit' ], 
})

registerTemplateFilter('TemplateTechnical', _('Template: Technical'), _('Page has \'too technical\' template set.'), None, {
    'dewiki_p': [ 'Allgemeinverständlichkeit' ],
    'enwiki_p': [ 'Technical' ],
    'frwiki_p': [ 'Article_incompréhensible' ],
    'cswiki_p': [ '' ],
})

registerTemplateFilter('TemplateGlobalize', _('Template: Globalize'), _('Page has \'globalize\' template set.'), _('Completeness'), {
    'dewiki_p': [ 'Staatslastig' ],
    'enwiki_p': [ 'Globalize' ],
    'frwiki_p': [ 'Internationaliser' ],
    'cswiki_p': [ 'Globalizovat' ],
})

# todo: extract template names of other languages from langlinks

# todo: gibt es für jedes wartungs-template eine kategorie analog zu http://de.wikipedia.org/wiki/Kategorie:Wikipedia:Neutralit%C3%A4t ? 
# wenn ja, dann könnte man den ganzen kram durch catgraph-anfragen ersetzen.
# in der deutschen wikipedia scheint jedem wartungstemplate eine Kategorie:Wikipedia:Wartungstemplate zu entsprechen.
# in der englischen scheint es was ähnliches zu geben, z. b. gibt es da http://en.wikipedia.org/wiki/Category:Articles_to_be_split .
# mir ist nicht klar, ob die kategorien automatisch hinzugefügt werden, sobald jemand das entsprechende template setzt.
# es gibt auch die möglichkeit, nach '+Wikipedia:Wartungskategorie' mit filter 'All' zu suchen.


## 
# todo: make results usable by both small + large filter
class FPageSizeBase(FlawFilter):
    
    class Action(TlgAction):
        def execute(self, resultQueue):            
            cur= getCursors()[self.wiki]
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            cur.execute('SELECT page_id, page_len FROM page WHERE page_id IN (%s) AND page_namespace=0 AND page_is_redirect=0' % format_strings, self.pageIDs)
            rows= cur.fetchall()
            try:
                self.parent.pageLengthLock.acquire()
                for row in rows:
                    pagelen= row['page_len']
                    self.parent.pageLengths[row['page_id']]= pagelen
                    self.parent.lengthSum+= pagelen
                self.parent.pagesLeft-= len(self.pageIDs)
            finally:
                self.parent.pageLengthLock.release()

    class FinalAction(TlgAction):
        # todo: the final action stuff takes a while, maybe this can be optimized.
        def execute(self, resultQueue):
            pageLengths= self.parent.pageLengths
            self.avg= self.parent.lengthSum / float(len(pageLengths))
            sum= 0
            for i in pageLengths:
                delta= pageLengths[i]-self.avg
                sum+= delta*delta
            self.stddev= math.sqrt(sum/len(pageLengths))
            dprint(3, "FTPageSize.FinalAction.execute() lengthSum = %d pages = %d avg length = %f stddev = %f" % (self.parent.lengthSum, len(pageLengths), self.avg, self.stddev))

        def canExecute(self):
            return self.parent.pagesLeft == 0
            
    def __init__(self, tlg):
        FlawFilter.__init__(self, tlg)
        self.pagesLeft= len(tlg.getPageIDs())
        self.finalActionCreated= False
        self.pageLengths= {}
        self.pageLengthLock= threading.Lock()
        self.lengthSum= 0
    
    def getPreferredPagesPerAction(self):
        return 50
    
    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))
        if not self.finalActionCreated: 
            actionQueue.put(self.FinalAction(self, language, self.tlg.getPageIDs))
            self.finalActionCreated= True

class FSmall(FPageSizeBase):
    shortname= 'Small'
    label= _('Small Pages')
    description= _('Page is very small, relative to mean page size in result set.')
    
    class FinalAction(FPageSizeBase.FinalAction):
        # todo: the final action stuff takes a while, maybe this can be optimized.
        def execute(self, resultQueue):
            pageLengths= self.parent.pageLengths
            if len(pageLengths):
                self.avg= self.parent.lengthSum / float(len(pageLengths))
                threshold= self.avg/4
                for i in pageLengths:
                    #~ delta= pageLengths[i]-self.avg
                    #~ if delta > self.stddev*8:
                        #~ resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, i)[0], self.parent))
                    if pageLengths[i] < threshold:
                        resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, i)[0], self.parent, '%d bytes' % pageLengths[i], sortkey= pageLengths[i]))

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(FPageSizeBase.Action(self, language, pages))
        if not self.finalActionCreated: 
            actionQueue.put(self.FinalAction(self, language, self.tlg.getPageIDs))
            self.finalActionCreated= True

FlawFilters.register(FSmall)


class FLarge(FPageSizeBase):
    shortname= 'Large'
    label= _('Large Pages')
    description= _('Page is very large, relative to mean page size in result set.')
    
    class FinalAction(FPageSizeBase.FinalAction):
        # todo: the final action stuff takes a while, maybe this can be optimized.
        def execute(self, resultQueue):
            FPageSizeBase.FinalAction.execute(self, resultQueue)
            pageLengths= self.parent.pageLengths
            for i in pageLengths:
                delta= pageLengths[i]-self.avg
                if delta > self.stddev*5:
                    resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, i)[0], self.parent, infotext= '%d bytes' % pageLengths[i], sortkey= -pageLengths[i]))

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(FPageSizeBase.Action(self, language, pages))
        if not self.finalActionCreated: 
            actionQueue.put(self.FinalAction(self, language, self.tlg.getPageIDs))
            self.finalActionCreated= True

FlawFilters.register(FLarge)




## 
class FNoImages(FlawFilter):
    shortname= 'NoImages'
    label= _('No Images')
    description= _('Article has no image links.')

    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            # IN stuff possibly makes this slow...
            sqlstr= """SELECT * FROM page WHERE page_namespace=0 AND page_id IN (%s) AND page_is_redirect = 0 
                AND page_id NOT IN (select il_from FROM imagelinks AS src WHERE il_from IN (%s) 
                    AND NOT EXISTS (SELECT 1 FROM imagelinks WHERE il_to=src.il_to AND il_from IN (SELECT page_id FROM page WHERE page_namespace=10)));""" % \
                    (format_strings, format_strings)
                    #~ AND (SELECT COUNT(*) FROM imagelinks WHERE il_to=src.il_to AND il_from IN (SELECT page_id FROM page WHERE page_namespace=10) LIMIT 1)=0);""" % \
            dblpages= copy.copy(self.pageIDs)
            dblpages.extend(self.pageIDs)
            cur.execute(sqlstr, dblpages)
            sqlres= cur.fetchall()

            for row in sqlres:
                resultQueue.put(TlgResult(self.wiki, row, self.parent))


    def getPreferredPagesPerAction(self):
        return 100

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FNoImages)


class FLonely(FlawFilter):
    shortname= 'Lonely'
    label= _('No Links to this article')
    description= _('Article is not linked from any other article.')

    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            format_strings = ' OR '.join(['page_id=%s'] * len(self.pageIDs))
            
            sqlstr= """SELECT * FROM page WHERE (%s) AND page_namespace=0 AND page_is_redirect=0 
                AND NOT EXISTS (SELECT 1 FROM pagelinks WHERE pl_title=page_title AND pl_namespace=0)""" \
                % (format_strings)
                #~ AND (SELECT COUNT(*) FROM pagelinks WHERE pl_from=page_id AND pl_namespace=0 LIMIT 1)=0""" 
            cur.execute(sqlstr, self.pageIDs)
            sqlres= cur.fetchall()
            
            for row in sqlres:
                resultQueue.put(TlgResult(self.wiki, row, self.parent))


    def getPreferredPagesPerAction(self):
        return 100

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FLonely)



class FPendingChanges(FlawFilter):
    shortname= 'PendingChanges'
    label= _('Pending Changes (12h)')
    description= _('Article has pending changes older than 12 hours.')

    # our action class
    class Action(TlgAction):
        def __init__(self, parent, language, pages, timestamp):
            TlgAction.__init__(self, parent, language, pages)
            self.timestamp= timestamp
        
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]

            # get the page IDs of pages with pending changes first
            format_strings= ' OR '.join(['fp_page_id=%s'] * len(self.pageIDs))
            sqlstr= "SELECT fp_page_id FROM flaggedpages WHERE (%s) AND fp_reviewed=0 AND fp_pending_since < %s" % (format_strings, self.timestamp)
            #~ dprint(3, sqlstr)
            cur.execute(sqlstr, self.pageIDs)
            res= cur.fetchall()
            foundPageIDs= []
            for row in res:
                foundPageIDs.append(row['fp_page_id'])
            
            if len(foundPageIDs):
                # then get info about the found articles from the page table
                format_strings= ' OR '.join(['page_id=%s'] * len(foundPageIDs))
                sqlstr= """SELECT * FROM page WHERE page_namespace=0 AND page_is_redirect=0 AND (%s)""" % (format_strings)
                dprint(3, sqlstr)
                cur.execute(sqlstr, foundPageIDs)
                sqlres= cur.fetchall()
                
                for row in sqlres:
                    resultQueue.put(TlgResult(self.wiki, row, self.parent))


    def getPreferredPagesPerAction(self):
        return 100

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages, MakeMWTimestamp(time.time()-60*60*12)))

FlawFilters.register(FPendingChanges)


