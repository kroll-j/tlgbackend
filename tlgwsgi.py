#!/usr/bin/python
# task list generator - wsgi interface to the backend
import os
import sys
import time
import json


# general procedure of things:
# - redirect stdout and stderr to different tmp files (or file-like objects that write to memory)
# - if TaskListGenerator.run() throws or returns False, start 5xx error response and return stderr file
# - else start 200 OK response and return stdout file

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
    
    action= params['action']
    if action=='query':
        #~ tlgbackend.TaskListGenerator().run(lang=params['lang'], queryString=params['query'], queryDepth=params['querydepth'], flaws=params['flaws'])
        tlgResult= tlgbackend.TaskListGenerator().generateQuery(lang=params['lang'], queryString=params['query'], queryDepth=params['querydepth'], flaws=params['flaws'])
    elif action=='listflaws':
        tlgResult= (tlgbackend.TaskListGenerator().listFlaws(),)   #xxx
    
    #~ sys.stdout= oldStdout
    #~ sys.stderr= oldStderr
    
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
        <body>
        <p id="thestatus">Status</p>
        """)
        
        import tlgflaws
        
        def resGen():
            for line in tlgResult:  # stdout.values:
                if len(line.split()):   # don't try to json-decode empty lines
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
                        html.write('<a href="https://%s.wikipedia.org/wiki/%s">%s</a> page_id=%d' % (params['lang'], title, title, data['page']['page_id']))
                        html.write('</td>')
                        html.write('</tr>\n')
                    elif 'status' in data:
                        html.write('</body><head><script type="text/javascript">document.getElementById("thestatus").innerHTML="%s"</script></head><body>' % str(data['status']))
                        #~ html.write('<script type="text/javascript">document.open(); document.write("%s");</script>' % str(data['status']))
                    else:
                        html.endTable()
                        html.write(line)
                while len(html.values)>0:
                    yield html.values.pop(0) + '\n'
            
            html.endTable()
            html.write('</body></html>')
            while len(html.values)>0:
                yield html.values.pop(0) + '\n'
        
        return resGen()
        
    else:
        # return json data
        start_response('200 OK', [('Content-Type', 'text/plain')])
        return tlgResult    #stdout.values

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
                    html.write('<a href="https://%s.wikipedia.org/wiki/%s">%s</a> page_id=%d' % (params['lang'], title, title, data['page']['page_id']))
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
    WSGIServer(myapp).run()
