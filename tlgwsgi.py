#!/usr/bin/python
# task list generator - wsgi interface to the backend
import os
import re
import sys
import time
import json
import gettext
import threading
import traceback
import tlgbackend
import tlgflaws
import utils
from utils import *



# general procedure of things (not accurate any more):
# - redirect stdout and stderr to different tmp files (or file-like objects that write to memory)
# - if TaskListGenerator.run() throws or returns False, start 5xx error response and return stderr file
# - else start 200 OK response and return stdout file

# todo: maybe some more cleanup

class FileLikeList:
    def __init__(self):
        self.values= []
        
    def close(self):
        pass
    
    def flush(self):
        pass
    
    def isatty(self):
        return False
    
    def write(self, str):
        self.values.append(str)

def addLinebreaks(iterable):
    for stuff in iterable:
        yield stuff + '\n'

def parseCGIargs(environ):
    from urllib import unquote
    from urlparse import parse_qs
    params= {}
    if 'CONTENT_LENGTH' in environ and int(environ['CONTENT_LENGTH'])!=0:
        dprint(0, "POST request, content length %s" % environ['CONTENT_LENGTH'])
        request_body= environ['wsgi.input'].read(int(environ['CONTENT_LENGTH']))
        params= parse_qs(request_body.replace(';', '%3B').replace('+', '%2B'))  # HACK to 'fix' wrongly encoded query string
    elif 'QUERY_STRING' in environ:
        params= parse_qs(environ['QUERY_STRING'].replace(';', '%3B').replace('+', '%2B'),   # HACK to 'fix' wrongly encoded query string
            strict_parsing= True)
        logStats(params)
    return params

def getParam(params, name, default= None):
    if name in params: return params[name][0]
    else: return default

def getBoolParam(params, name, default= None):
    p= getParam(params, name, default)
    if str(p).lower() in ['true', 'on']: return True
    return default


## send email from background process
def send_mail(queryString, queryDepth, flaws, lang, format, outputIterable, action, mailto, mimeSubtype):
    import mail, socket
    
    attachmentText= ''
    for i in outputIterable:
        attachmentText+= i.decode('utf-8')
    actionText= '\taction: %s\n' % action
    if action=='query':
        actionText+= "\tLanguage: '%s'\n" % lang + \
            "\tCatGraph query string: '%s', with recursion depth %s\n" % (queryString, queryDepth) + \
            "\tSearch filters: '%s'" % flaws
    elif action=='listflaws':
        pass
    msgText= (_("""Hi!

This is the Wikimedia task list generator background process running on %s. 
You (or someone else) requested a task list to be generated and sent to this email address. 
If you think this mail was generated in error, something went wrong, or you have suggestions 
for the TLG, send email to jkroll@toolserver.org.

The task list was successfully generated. Input was:

""") % socket.getfqdn() + actionText + _("""

Attached is the result of the command in %s format.

Sincerely, 
The friendly task list generator robot. 
""") % format)
    mail.sendFriendlyBotMessage(mailto, msgText, attachmentText, mimeSubtype)
    
def HTMLify(tlgResult, action, chunked, params, showThreads, tlg):
    class htmlfoo(FileLikeList):
        def __init__(self):
            FileLikeList.__init__(self)
            self.currentTableType= None
        def endTable(self):
            if self.currentTableType!=None: 
                self.write('</table>\n')
                self.currentTableType= None
        def startTable(self, tableType):
            if tableType!=self.currentTableType:
                self.endTable()
                self.write('<table cellpadding=8 rules="all" style="border-width:1px; border-style:solid; ">\n')
                if tableType=='flaws':
                    self.write('<tr>')
                    self.write('<th align="left">' + _('Filters') + '</th>')
                    self.write('<th align="left">' + _('Page') + '</th>')
                    self.write('</tr>\n')
                self.currentTableType= tableType
    
    html= htmlfoo()
    html.write("""<html><head><title>Task List</title>
<style type="text/css">
table { font-family: Sans; font-size: 10.5pt; width: 100%; margin-top: 8px; }
.nobr { white-space: nowrap; width: 150px; }
.meter-wrap { position: relative; }
.meter-wrap, .meter-value, .meter-text {
width: 100%; 
height: 25px;
}

.meter-wrap, .meter-value {
/* background: #bdbdbd url(/path/to/your-image.png) top left no-repeat; */
}

.meter-text {
position: absolute;
top:0; left:0;
text-align: center;
padding-top: 5px;
padding-bottom: 5px;
width: 100%;
font-size: 11px;
}
</style>
</head>
<body>
""")
    if chunked:
        html.write("""
<div class="meter-wrap" style="background-color: #F0E0E0;">
<div class="meter-value" id="status-meter-value" style="background-color: #60FF80; width: 0%;">
    <div class="meter-text" id="thestatus" style="font-family: Monospace;">Status</div>
</div>
</div>
<script type="text/javascript">
function setMeter(percent) { document.getElementById("status-meter-value").style.width=percent; }
function setStatus(text, percentage) { document.getElementById("thestatus").innerHTML=text; if(percentage>=0) setMeter(percentage + '%'); }
</script>
""")
    

    def getCurrentActions():
        if tlg.getActiveWorkerCount()<1: return ''
        r= '<div style=\\"text-align: left; position: absolute; top: 34px; left: 0px; white-space: pre; font-size: 9.5px;\\">Threads:<br>'
        i= 0
        for t in tlg.workerThreads:
            r+= "%2d: %s<br>" % (i, t.getCurrentAction())
            i+= 1
        return r + '</div>'
    
    results= 0
    for line in tlgResult:
        if len(line.split()):   # don't try to json-decode empty lines
            data= json.loads(line)
                                
            if action=='query' and 'flaws' in data:
                results+= 1
                html.startTable('flaws')
                html.write('<tr>')
                html.write('<td class="nobr">')
                for filter in data['flaws']:
                    html.write(filter['name'])
                    if filter['infotext']: html.write(' (%s)' % filter['infotext'])
                    if filter['hidden']: html.write(_(' [hidden]'))
                    html.write('<br/>')
                html.write('</td>')
                html.write('<td>')
                title= data['page']['page_title'].encode('utf-8')
                html.write('<a href="https://%s.wikipedia.org/wiki/%s">%s</a>' % (params['lang'], title, title))
                #~ html.write(' page_id = %d' % (data['page']['page_id']))
                html.write('</td>')
                html.write('</tr>\n')
            elif 'status' in data:
                lastStatus= data['status'].encode('utf-8')
                if chunked:
                    if showThreads: statusText= lastStatus + getCurrentActions()
                    else: statusText= lastStatus
                    html.write('<script>setStatus("%s", %d)</script>' % (statusText, -1))
            elif 'progress' in data:
                if chunked:
                    match= re.match('[^0-9]*([0-9]+)/([0-9]+).*', data['progress'])
                    percentage= -1
                    if match and int(match.group(2))!=0:
                        percentage= int(match.group(1))*100/int(match.group(2))
                        html.write('<script>setMeter("%d%%")</script>' % (percentage))
            else:
                html.endTable()
                html.write(line)
        while len(html.values)>0:
            yield html.values.pop(0) + '\n'
    
    yield '<script>setMeter("100%");</script>'
    
    if results==0:
        html.startTable('flaws')
        html.write('<tr>')
        html.write('<td class="nobr">No results.</td>')
        html.write('<td></td>')
    
    html.endTable()
    html.write('</body></html>')
    while len(html.values)>0:
        yield html.values.pop(0) + '\n'

def Wikify(tlgResult, action, chunked, params, showThreads, tlg):
    class wikifoo(FileLikeList):
        def __init__(self):
            FileLikeList.__init__(self)
            self.currentTitle= None
        def endHeading(self):
            if self.currentTitle!=None: 
                self.write('')
                self.currentTitle= None
        def startHeading(self, title):
            if title!=self.currentTitle:
                self.endHeading()
                self.write('%s' % title)
                self.currentTitle= title
    
    wikitext= wikifoo()
    
    results= 0
    for line in tlgResult:
        if len(line.split()):   # don't try to json-decode empty lines
            data= json.loads(line)
            
            if action=='query' and 'flaws' in data:
                results+= 1
                flaws= ''
                for filter in data['flaws']: 
                    if flaws!='': flaws+= ', '
                    flaws+= filter['name']
                    #~ if filter['infotext']: flaws+= ' (%s)' % filter['infotext']      # TODO new formatting
                    #~ if filter['hidden']: flaws+= _(' [hidden]')
                title= data['page']['page_title'].encode('utf-8')
                wikitext.startHeading('== %s ==' % flaws)   # TODO with the new formatting, this doesn't work any more... 
                wikitext.write('* [[%s]]' % title)
            elif 'status' in data or 'progress' in data:
                pass
            else:
                # output unknown stuff too?
                #~ pass
                wikitext.write(str(data))
            
        while len(wikitext.values)>0:
            yield wikitext.values.pop(0) + '\n'
        
    if results==0:
        yield 'No Results.\n'


def makeHelpPage():
    import gp
    gp= gp.client.Connection( gp.client.ClientTransport(config['graphserv-host'], config['graphserv-port']) )
    try:
        gp.connect()
        runningGraphs= ''
        for i in gp.capture_list_graphs():
            if 'wiki' in i[0]:
                if len(runningGraphs): runningGraphs+= ', '
                runningGraphs+= i[0].split('wiki')[0]
    except Exception as ex:
        runningGraphs= str(ex)
    
    class htmlfoo(FileLikeList):
        def __init__(self):
            FileLikeList.__init__(self)
            self.currentTableType= None

    header= '<html><head><title>TLG backend</title></head><body>'
    footer= '</body></html>'

    html= htmlfoo()
    
    html.write(header)

    helptext= """TLG backend parameters:<pre>
* action
    * action=listflaws -- list available flaw filters
    * action=query -- query CatGraph for categories and filter articles
        * lang=&lt;string> -- wiki language code ('de', 'en').
            graphcore instances are currently running for <a href="http://ortelius.toolserver.org:8090/list-graphs">""" + str(runningGraphs) + """</a>.
        * query=&lt;string> -- execute a search-engine style query string using CatGraph. 
            separate category names by semicolons. operators '+' (intersection) and '-' (difference) are supported.
            e. g. "Biology; Art; +Apes; -Cats" searches for everything in Biology or Art and in Apes, not in Cats
            search parameters are evaluated from left to right, i.e. results might differ depending on order.
            on the first category, any '+' operator is ignored, while a '-' operator yields an empty result.
        * querydepth=&lt;integer> -- recursion depth for the search. applied to each category.
        * flaws=&lt;string> -- space-separated list of filters ("listflaws" for possible filters). 
        * format=&lt;string> -- select output format. possible values are 
            * html - HTML format mostly used for debugging
            * json - one JSON dict per line. possible content:
                * {"status": "localized human-readable text"}
                    just some status text to show what's going on
                * {"progress": "processed/total"}
                    number of total and already processed actions. can be used to show some form of progress bar
                * {"flaws": ["filter1", "filter2"], "page": {"page_title": "title", "page_id": int, "page_namespace": 0, .... }}
                    filtered page result. "flaws" lists one or more filters which identified the page, "page" is the complete 
                    entry from the <a href="http://www.mediawiki.org/wiki/Page_table">page table</a>.
            * wikitext - this format can be used to copy to user pages or similar.
* i18n=&lt;language code> -- select output language ('de', 'en')
* chunked=true -- if specified, use chunked transfer encoding. for creating dynamic progress bars and the like.
* showthreads=true -- debug output; show what threads are doing. use with format=html + chunked=true.
</pre>""";

    html.write(helptext)

    html.write(footer)
    
    while len(html.values)>0:
        yield html.values.pop(0) + '\n'
    
    
    

############## wsgi generator function
def generator_app(environ, start_response):
    try:
        params= parseCGIargs(environ)
        
        if len(params)==0:
            start_response('200 OK', [('Content-Type', 'text/html; charset=utf-8')])
            return makeHelpPage()
        
        mailto= getParam(params, 'mailto', None)
        chunked= getBoolParam(params, 'chunked', False) and not bool(mailto)
        showThreads= getBoolParam(params, 'showthreads', False) and not bool(mailto)
        action= getParam(params, 'action', 'listflaws')
        format= getParam(params, 'format', 'json')
        if mailto and format=='json': format= 'html'    # no json via email (probably not useful anyway)
        i18n= getParam(params, 'i18n', 'de')
        wikipage= getParam(params, 'wikipage', None)
        if wikipage: format= 'wikitext' # writing to wiki page implies wikitext format
        utils.testrun= getBoolParam(params, 'test', False)
        
        dprint(0, "testrun: %s" % testrun);
        logStats({'environment': str(environ)})

        try:
            gettext.translation('tlgbackend', localedir= os.path.join(sys.path[0], 'messages'), languages=[i18n]).install()
        except:
            # fall back to untranslated strings
            def ident(msg): return msg
            global _
            _= ident
        
        if mailto or wikipage:
            if 'daemon' in environ and environ['daemon']=='True':
                # we are in the background process, open daemon context
                import daemon
                context= daemon.DaemonContext()
                context.stdout= open(os.path.join(DATADIR, 'mailer-stdout'), 'a')
                context.stderr= open(os.path.join(DATADIR, 'mailer-stderr'), 'a')
                context.open()
                #~ dprint(0, 'hello from background process, pid=%d, format=%s' % (os.getpid(), format))
                logStats({'backgroundprocess_pid': os.getpid(), 'backgroundProcessOutputFormat': format})

            else:
                # cgi context, create background process
                import subprocess
                scriptname= os.path.join(sys.path[0], sys.argv[0])
                subprocess.Popen([scriptname], env= { 'QUERY_STRING': environ['QUERY_STRING'], 'daemon': 'True' })
                start_response('200 OK', [('Content-Type', 'text/plain; charset=utf-8')])
                return ( '{ "status": "background process started" }', )
        
        tlg= tlgbackend.TaskListGenerator()
        
        if action=='query':
            lang= getParam(params, 'lang')
            queryString= getParam(params, 'query')
            queryDepth= getParam(params, 'querydepth', 1)
            flaws= getParam(params, 'flaws')
            include_hidden= getBoolParam(params, 'include_hidden', False)
            if lang is None or queryString is None or flaws is None:
                raise InputValidationError("parameters lang, query, and flaws must be given")
            tlgResult= tlg.generateQuery(lang=lang, queryString=queryString, queryDepth=queryDepth, flaws=flaws, include_hidden=include_hidden)
        elif action=='listflaws':
            tlgResult= (tlg.getFlawList(),)
        elif action=='markasdone':
            page_id= getParam(params, 'page_id')
            page_title= getParam(params, 'page_title')
            page_latest= getParam(params, 'page_latest')
            filter_name= getParam(params, 'filter_name')
            unmark= getParam(params, 'unmark', False)
            if page_id==None or page_title==None or page_latest==None or filter_name==None:
                raise InputValidationError(_('page_id, page_title, page_latest and filter_name parameters are required'))
            
            tlg.markAsDone(page_id, page_title, page_latest, filter_name, unmark)
            start_response('200 OK', [('Content-Type', 'text/plain; charset=utf-8')])
            return ( '{ "status": "ok" }', )
        else:
            raise InputValidationError('unknown action %s requested' % action)
            
        
        if format=='html':
            # output something html-ish
            outputIterable= HTMLify(tlgResult, action, chunked, params, showThreads, tlg)
            mimeSubtype= 'html'
        
        elif format=='wikitext':
            # format to wikitext
            outputIterable= Wikify(tlgResult, action, chunked, params, showThreads, tlg)
            mimeSubtype= 'plain'
        
        else:
            # plain json
            outputIterable= addLinebreaks(tlgResult)
            mimeSubtype= 'plain'
        
        if mailto:      # we are in the daemon if we get here
            dprint(0, 'starting email')
            send_mail(queryString, queryDepth, flaws, lang, format, outputIterable, action, mailto, mimeSubtype)
            dprint(0, 'mail sent, exiting.')
            sys.exit(0)

        elif wikipage:  # we are in the daemon if we get here, write output to wiki page
            logStats( {'backgroundprocess_wikipage': wikipage} )
            import wiki
            wiki.SimpleMW(lang).writeToPage(queryString, queryDepth, flaws, outputIterable, action, wikipage)
            dprint(0, 'finished writing to wiki page \'%s\'' % wikipage)
            sys.exit(0)

        else:   # no email address or wiki page given. normal cgi context.
            if chunked: 
                start_response('200 OK', [('Content-Type', 'text/%s; charset=utf-8' % mimeSubtype), ('Transfer-Encoding', 'chunked')])
            else:
                start_response('200 OK', [('Content-Type', 'text/%s; charset=utf-8' % mimeSubtype)])
            return outputIterable
    
    except Exception as e:
        info= sys.exc_info()
        dprint(0, traceback.format_exc(info[2]))
        start_response('200 OK', [('Content-Type', 'text/plain; charset=utf-8')])
        return [ '{"exception": "%s"}' % (traceback.format_exc(info[2]).replace('\n', '\\n').replace('"', '\\"')) ]



if __name__ == "__main__":
    getRequestID()
    
    if 'daemon' in os.environ and os.environ['daemon']=='True':
        for foo in generator_app(os.environ, None):
            pass
        sys.exit(0)
    
    # change this to "flup.server.fcgi" when/if fcgid is installed on the toolserver
    from flup.server.cgi import WSGIServer
    # enable pretty stack traces
    #~ import cgitb
    #~ cgitb.enable()
    #~ if (not 'QUERY_STRING' in os.environ) and (not 'CONTENT_LENGTH' in os.environ):
        #~ # started from non-cgi context, create request string for testing.
        #~ os.environ['QUERY_STRING']= 'action=query&format=html&chunked=true&lang=de&query=Sport&querydepth=2&flaws=NoImages%20Small'
        #~ os.environ['QUERY_STRING']= 'action=query&format=wikitext&lang=de&query=Sport&querydepth=2&flaws=Small'
    #~ dprint(str(os.environ))
    WSGIServer(generator_app).run()


