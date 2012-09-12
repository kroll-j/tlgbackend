#!/usr/bin/python
# -*- coding:utf-8 -*-
import time
from tlgflaws import *

## 
class FAll(FlawFilter):
    shortname= 'ALL'
    label= _('All Pages')
    description= _('Every page from CatGraph result.')
    
    # our action class
    class Action(TlgAction):
        def execute(self, resultQueue):
            cur= getCursors()[self.wiki]
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            cur.execute('SELECT * FROM page WHERE page_id IN (%s)' % format_strings, self.pageIDs)
            result= cur.fetchall()
            for row in result:
                resultQueue.put(TlgResult(self.wiki, row, self.parent))
    
    def getPreferredPagesPerAction(self):
        return 100

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FAll)

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
            dprint(3, "%s: execute begin" % (self.parent.description))
                        
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

            dprint(3, "%s: execute end" % (self.parent.description))

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

registerTemplateFilter('TemplateNeutrality', _('Neutrality Template'), _('Page has \'neutrality\' template set.'), None, {
    'dewiki_p': [ 'Neutralität' ],
    'enwiki_p': [ 'Neutrality' ],
})

registerTemplateFilter('TemplateMissingSources', _('Missing Sources/References Template'), _('Page has \'missing sources\' template set.'), None, {
    'dewiki_p': [ 'Belege_fehlen' ],
    'enwiki_p': [ 'Refimprove' ]
})

registerTemplateFilter('Timeliness:TemplateObsolete', _('Obsolete Template'), _('Page has \'obsolete\' template set.'), _('Timeliness'), {
    'dewiki_p': [ 'Veraltet' ],
})

registerTemplateFilter('TemplateCleanup', _('Cleanup Template'), _('Page has \'cleanup\' template set.'), None, {
    'dewiki_p': [ 'Überarbeiten' ],
    'enwiki_p': [ 'Cleanup' ],
})

registerTemplateFilter('TemplateTechnical', _('\'Too Technical\' Template'), _('Page has \'too technical\' template set.'), None, {
    'dewiki_p': [ 'Allgemeinverständlichkeit' ],
    'enwiki_p': [ 'Technical' ],
})

registerTemplateFilter('TemplateGlobalize', _('Globalize Template'), _('Page has \'globalize\' template set.'), None, {
    'dewiki_p': [ 'Staatslastig' ],
    'enwiki_p': [ 'Globalize' ],
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
            #~ FPageSizeBase.FinalAction.execute(self, resultQueue)
            pageLengths= self.parent.pageLengths
            self.avg= self.parent.lengthSum / float(len(pageLengths))
            threshold= self.avg/4
            for i in pageLengths:
                #~ delta= pageLengths[i]-self.avg
                #~ if delta > self.stddev*8:
                    #~ resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, i)[0], self.parent))
                if pageLengths[i] < threshold:
                    resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, i)[0], self.parent))

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
                if delta > self.stddev*8:
                    resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, i)[0], self.parent))
                #~ if pageLengths[i] < self.avg / 4:
                    #~ resultQueue.put(TlgResult(self.wiki, getPageByID(self.wiki, i)[0], self.parent))

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
            dprint(3, "%s: execute begin" % (self.parent.description))
            
            cur= getCursors()[self.wiki]
            format_strings = ','.join(['%s'] * len(self.pageIDs))
            # find all pages in set without any image links
            # the double 'IN' clause seems fishy, maybe i will figure out a better way to do this in sql some time. 
            sqlstr= 'SELECT * FROM page WHERE page_id IN (%s) AND page_namespace=0 AND page_is_redirect=0 AND page_id NOT IN (SELECT il_from FROM imagelinks WHERE il_from IN (%s))' % \
                (format_strings,format_strings)
            dblpages= copy.copy(self.pageIDs)
            dblpages.extend(self.pageIDs)
            cur.execute(sqlstr, dblpages)
            sqlres= cur.fetchall()
            
            for row in sqlres:
                resultQueue.put(TlgResult(self.wiki, row, self.parent))
            
            dprint(3, "%s: execute end" % (self.parent.description))

    def getPreferredPagesPerAction(self):
        return 100

    def createActions(self, language, pages, actionQueue):
        actionQueue.put(self.Action(self, language, pages))

FlawFilters.register(FNoImages)


