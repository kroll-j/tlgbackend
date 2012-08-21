#!/usr/bin/python
# task list generator - wsgi interface to the backend
import os
import re
import sys
import time
import json


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

def generator_test(environ, start_response):
    #~ start_response('200 OK', [('Content-Type', 'text/html; charset=utf-8')])
    #~ return htmlgen()
    
    #~ oldStdout= sys.stdout
    #~ oldStderr= sys.stderr
    #~ stdout= FileLikeList()
    #~ stderr= FileLikeList()
    #~ sys.stdout= stdout
    #~ sys.stderr= stderr
    
    import tlgbackend
    from urllib import unquote
    
    #~ try:
    params= {}
    for param in environ['QUERY_STRING'].split('&'):
        blah= param.split('=')
        params[blah[0]]= unquote(blah[1])
    
    chunked= False
    if 'chunked' in params and params['chunked'].lower() in ['true', 'on']: chunked= True
    
    action= params['action']
    if action=='query':
        tlgResult= tlgbackend.TaskListGenerator().generateQuery(lang=params['lang'], queryString=params['query'], queryDepth=params['querydepth'], flaws=params['flaws'])
    elif action=='listflaws':
        tlgResult= (tlgbackend.TaskListGenerator().getFlawList(),)
    
    if 'format' in params and params['format']=='html':
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
        
        import tlgflaws
        
        def resGen():
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
                        if chunked:
                            match= re.match('[^0-9]*([0-9]+) of ([0-9]+).*', str(data['status']))
                            percentage= -1
                            if match:
                                percentage= int(match.group(1))*100/int(match.group(2))
                            html.write('<script>setStatus("%s", %d)</script>' % (str(data['status']), percentage))
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
        
        if chunked: 
            start_response('200 OK', [('Content-Type', 'text/html; charset=utf-8'), ('Transfer-Encoding', 'chunked')])
        else:
            start_response('200 OK', [('Content-Type', 'text/html; charset=utf-8')])
        return resGen()
        
    else:
        # return json data
        if chunked: 
            start_response('200 OK', [('Content-Type', 'text/plain; charset=utf-8'), ('Transfer-Encoding', 'chunked')])
        else:
            start_response('200 OK', [('Content-Type', 'text/plain')])
        return addLinebreaks(tlgResult)

    #~ except Exception as ex:
        #~ sys.stdout= oldStdout
        #~ sys.stderr= oldStderr
        #~ start_response('504 Foobar', [('Content-Type', 'text/plain')])
        #~ return [str(ex), stderr.values]


def myapp(environ, start_response):
    oldStdout= sys.stdout
    oldStderr= sys.stderr
    stdout= FileLikeList()
    stderr= FileLikeList()
    sys.stdout= stdout
    sys.stderr= stderr
    
    import tlgbackend
    from urllib import unquote
    
    #~ try:
    params= {}
    for param in environ['QUERY_STRING'].split('&'):
        blah= param.split('=')
        params[blah[0]]= unquote(blah[1])
    
    action= params['action']
    if action=='query':
        tlgbackend.TaskListGenerator().run(lang=params['lang'], queryString=params['query'], queryDepth=params['querydepth'], flaws=params['flaws'])
    elif action=='listflaws':
        tlgbackend.TaskListGenerator().listFlaws()
    
    sys.stdout= oldStdout
    sys.stderr= oldStderr
    
    if 'format' in params and params['format']=='html':
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
                        self.write('<th align="left">Flaws</th>')
                        self.write('<th align="left">Page</th>')
                        self.write('</tr>\n')
                    self.currentTableType= tableType
        
        start_response('200 OK', [('Content-Type', 'text/html; charset=utf-8')])
        html= htmlfoo()
        html.write("""<html><head><title>Task List</title>
<style type="text/css">
table { font-family: Sans; font-size: 10.5pt; }
</style>
</head>
<body>""")
        
        import tlgflaws
        
        for line in stdout.values:
            if len(line.split()): # don't try to json-decode empty lines
                data= json.loads(line)
                if action=='query' and 'flaws' in data:
                    html.startTable('flaws')
                    html.write('<tr>')
                    html.write('<td>')
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
                else:
                    html.endTable()
                    html.write(line)
        
        html.endTable()
        html.write('</body></html>')
        return html.values
    else:
        # return json data
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return stdout.values

    #~ except Exception as ex:
        #~ sys.stdout= oldStdout
        #~ sys.stderr= oldStderr
        #~ start_response('504 Foobar', [('Content-Type', 'text/plain')])
        #~ return [str(ex), stderr.values]


if __name__ == "__main__":
    # change this to "flup.server.fcgi" when/if fcgid is installed on the toolserver
    from flup.server.cgi import WSGIServer
    # enable pretty stack traces
    import cgitb
    cgitb.enable()
    WSGIServer(generator_test).run()
