#!/usr/bin/python
# task list generator - wsgi interface to the backend
import os
import re
import sys
import time
import json
import threading
import tlgflaws
from utils import *


# general procedure of things:
# - redirect stdout and stderr to different tmp files (or file-like objects that write to memory)
# - if TaskListGenerator.run() throws or returns False, start 5xx error response and return stderr file
# - else start 200 OK response and return stdout file

# todo: cleanup

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

def getParam(params, name, default= None):
    if name in params: return params[name]
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
    msgText= ("""Hi!

This is the Wikimedia task list generator background process running on %s. 
You (or someone else) requested a task list to be generated and sent to this email address. 
If you think this mail was generated in error, something went wrong, or you have suggestions 
for the TLG, send email to jkroll@toolserver.org.

The task list was successfully generated. Input was:

""" % socket.getfqdn() + actionText + """

Attached is the result of the command in %s format.

Sincerely, 
The friendly task list generator robot. 
""" % format)
    mail.sendFriendlyBotMessage(mailto, msgText, attachmentText, mimeSubtype)
    
def parseCGIargs(environ):
    from urllib import unquote
    params= {}
    if 'QUERY_STRING' in environ:
        for param in environ['QUERY_STRING'].split('&'):
            blah= param.split('=')
            params[blah[0]]= unquote(blah[1])
    return params

def generator_test(environ, start_response):
    import tlgbackend

    params= parseCGIargs(environ)
    
    mailto= getParam(params, 'mailto', None)
    chunked= getBoolParam(params, 'chunked', False) and not bool(mailto)
    showThreads= getBoolParam(params, 'showthreads', False) and not bool(mailto)
    action= getParam(params, 'action', 'listflaws')
    format= getParam(params, 'format', 'json')
    if mailto: format= 'html'   # todo: no json via email yet
    
    if mailto:
        if 'daemon' in environ and environ['daemon']=='True':
            # in daemon process
            import daemon
            context= daemon.DaemonContext()
            context.stdout= open(os.path.join(DATADIR, 'mailer-stdout'), 'a')
            context.stderr= open(os.path.join(DATADIR, 'mailer-stderr'), 'a')
            context.open()
            dprint(0, 'hello from daemon, pid=%d' % os.getpid())

        else:
            # cgi context, create daemon
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
        tlgResult= tlg.generateQuery(lang=lang, queryString=queryString, queryDepth=queryDepth, flaws=flaws)
    elif action=='listflaws':
        tlgResult= (tlg.getFlawList(),)
    
    if format=='html':
        # output something html-ish

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
                        self.write('<th align="left">Filters</th>')
                        self.write('<th align="left">Page</th>')
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
        
        lastStatus= ''
        
        def resGen():
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
                        for flaw in sorted(data['flaws']): 
                            html.write('<span title="%s">' % tlgflaws.FlawFilters.classInfos[flaw].description)
                            html.write(flaw + ' ')
                            html.write('</span>')
                        html.write('</td>')
                        html.write('<td>')
                        title= data['page']['page_title'].encode('utf-8')
                        #~ html.write('<a href="https://%s.wikipedia.org/wiki/%s">%s</a> page_id=%d' % (params['lang'], title, title, data['page']['page_id']))
                        html.write('<a href="https://%s.wikipedia.org/wiki/%s">%s</a>' % (params['lang'], title, title))
                        html.write('</td>')
                        html.write('</tr>\n')
                    elif 'status' in data:
                        lastStatus= str(data['status'])
                        if chunked:
                            match= re.match('[^0-9]*([0-9]+) of ([0-9]+).*', str(data['status']))
                            percentage= -1
                            if match and int(match.group(2))!=0:
                                percentage= int(match.group(1))*100/int(match.group(2))
                            if showThreads: statusText= str(data['status']) + getCurrentActions()
                            else: statusText= str(data['status'])
                            html.write('<script>setStatus("%s", %d)</script>' % (statusText, percentage))
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
        
        outputIterable= resGen()
        mimeSubtype= 'html'
    
    else:
        outputIterable= addLinebreaks(tlgResult)
        mimeSubtype= 'plain'
    
    if mailto:
        dprint(0, 'starting email stuff')
        send_mail(queryString, queryDepth, flaws, lang, format, outputIterable, action, mailto, mimeSubtype)
        dprint(0, 'mail stuff finished, exiting.')
        sys.exit(0)

    else:
        if chunked: 
            start_response('200 OK', [('Content-Type', 'text/%s; charset=utf-8' % mimeSubtype), ('Transfer-Encoding', 'chunked')])
        else:
            start_response('200 OK', [('Content-Type', 'text/%s; charset=utf-8' % mimeSubtype)])
        return outputIterable
        
        #~ if chunked: 
            #~ start_response('200 OK', [('Content-Type', 'text/html; charset=utf-8'), ('Transfer-Encoding', 'chunked')])
        #~ else:
            #~ start_response('200 OK', [('Content-Type', 'text/html; charset=utf-8')])
        #~ return resGen()
        
    #~ else:
        #~ # return json data
        #~ if chunked: 
            #~ start_response('200 OK', [('Content-Type', 'text/plain; charset=utf-8'), ('Transfer-Encoding', 'chunked')])
        #~ else:
            #~ start_response('200 OK', [('Content-Type', 'text/plain')])
        #~ return addLinebreaks(tlgResult)



if __name__ == "__main__":
    if 'daemon' in os.environ and os.environ['daemon']=='True':
        for foo in generator_test(os.environ, None):
            pass
        sys.exit(0)
    
    # change this to "flup.server.fcgi" when/if fcgid is installed on the toolserver
    from flup.server.cgi import WSGIServer
    # enable pretty stack traces
    import cgitb
    cgitb.enable()
    try:
        os.environ['QUERY_STRING']
    except KeyError:
        # started from non-cgi context, create request string for testing.
        os.environ['QUERY_STRING']= 'action=query&format=html&lang=de&query=Sport&querydepth=2&flaws=NoImages%20Small&mailto=jkroll@lavabit.com'
    WSGIServer(generator_test).run()


