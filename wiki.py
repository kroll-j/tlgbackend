#!/usr/bin/python
# -*- coding:utf-8 -*-
import os
from wikitools import wiki, api
from utils import *

class SimpleMW:
    ## constructor.
    # @param lang language ('de', 'en' etc)
    def __init__(self, lang):
        self.site= wiki.Wiki('http://%s.wikipedia.org/w/api.php' % lang)
        self.site.setUserAgent('TLGBackend/0.1 (http://toolserver.org/~render/stools/tlg)')
        self.site.cookiepath= os.path.expanduser('~')+'/.tlgbackend/'
        try: os.mkdir(self.site.cookiepath)
        except: pass    # assume it's already there
        self.edittoken= False
        self.login()
    
    ## login to the api.
    # uses cookie file to remember previous login.
    def login(self):
        if not self.site.isLoggedIn():
            dprint(1, 'login: %s' % self.site.login('tlgbackend', 'wck8#0g', remember= True))
        else:
            dprint(1, 'login: already logged in')

    ## get edit token.
    def getEditToken(self):
        if not self.edittoken: 
            params= {   'action': 'tokens',
                        'type': 'edit',
            }
            req= api.APIRequest(self.site, params)
            res= req.query(querycontinue=False)
            self.edittoken= res['tokens']['edittoken']
        return self.edittoken

    ## write query result to a wiki page. wip.
    def writeToPage(self, queryString, queryDepth, flaws, outputIterable, action, wikipage):
        edittext= ''
        edittext+= _('= Task List =\n')
        edittext+= _('Categories: %s, Search depth: %s, Selected filters: %s\n') % (queryString, queryDepth, flaws)
        
        for i in outputIterable: edittext+= str(i)

        params= {   'action': 'edit',
                    'title': wikipage,
                    'text': edittext,
                    'summary': 'edit summary.',
                    'bot': 'True',
                    'recreate': 'True',
                    'token': self.getEditToken(),
                }
        req= api.APIRequest(self.site, params, write= True)
        res= req.query(querycontinue= False)
        if not 'edit' in res or not 'result' in res['edit'] or res['edit']['result']!='Success':
            raise RuntimeError(str(res))
        return res
    
    ## get complete watchlist, starting from wlstart.
    # only retrieves pages in namespace 0 (articles).
    def getWatchlist(self, wlowner, wltoken, wlstart= None, wlend= None):
        params= {   'action': 'query',
                    'list': 'watchlist',
                    'wlowner': wlowner,
                    'wltoken': wltoken,
                    'wlprop': 'user|timestamp|title|ids|flags',
                    'wlallrev': 'true',
                    'wldir': 'older',
                    'wlnamespace': 0,
                }
        
        if wlstart: params['wlstart']= wlstart
        if wlend: params['wlend']= wlend

        req= api.APIRequest(self.site, params)
        res= req.query(querycontinue= True)
        #~ if not 'edit' in res or not 'result' in res['edit'] or res['edit']['result']!='Success':
            #~ raise RuntimeError(str(res))
        return res
    
    
    def getWatchlistPages(self, wlowner, wltoken, wlstart= None, wlend= None):
        wl= self.getWatchlist(wlowner, wltoken, wlstart, wlend)
        res= dict()
        for p in wl['query']['watchlist']:
            if p['pageid']!=0 and (not(p['pageid'] in res) or (res[p['pageid']]['timestamp'] < p['timestamp'])):
                res[p['pageid']]= p
        return res
    
        

if __name__ == '__main__':
    global _
    def ident(x): return x
    _= ident
    from pprint import pprint
    
    mw= SimpleMW('de')
    #~ print mw.writeToPage('query string', 3, 'filter1 filter2 filter3', ('foo\n\n', 'bar\n\n', 'baz\n\n', 'etc\n\n'), 'query', 'Benutzer:Tlgbackend/Foo')
    #~ pprint(mw.getWatchlist('Johannes Kroll (WMDE)', '5e936929bde94754ef270918c939cdd70d68cb5b'))
    pprint(mw.getWatchlistPages('Johannes Kroll (WMDE)', '5e936929bde94754ef270918c939cdd70d68cb5b'))
    
    sys.exit(0)
    
    
    import pprint # Used for formatting the output for viewing, not necessary for most code
    site= wiki.Wiki("http://de.wikipedia.org/w/api.php")
    
    print ' *** login'
    params= {   'action': 'login',
                'lgname': 'tlgbackend',
                'lgpassword': 'wck8#0g'
    }
    
    req= api.APIRequest(site, params)
    res= req.query(querycontinue=False)
    
    token= res['login']['token']

    print ' *** lgtoken'
    params= {   'action': 'login',
                'lgname': 'tlgbackend',
                'lgpassword': 'wck8#0g',
                'lgtoken': token,
    }
    
    req= api.APIRequest(site, params)
    res= req.query(querycontinue=False)
    
    print ' *** edittoken'
    params= {   'action': 'tokens',
                'type': 'edit',
    }
    
    req= api.APIRequest(site, params)
    res= req.query(querycontinue=False)
    pprint.pprint(res)
    
    edittoken= res['tokens']['edittoken']
    
    print ' *** edit'
    params= {   'action': 'edit',
                'title': 'Benutzer:Tlgbackend/Foo',
                'text': 'Test Text!',
                'summary': 'edit summary.',
                'bot': 'True',
                'recreate': 'True',
                'token': edittoken,
            }
    req= api.APIRequest(site, params, write= True)
    res= req.query(querycontinue=False)
    print(res)

