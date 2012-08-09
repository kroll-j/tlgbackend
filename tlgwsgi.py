#!/usr/bin/python
# task list generator - wsgi interface to the backend
import os
import sys


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
    
    

def myapp(environ, start_response):
    oldStdout= sys.stdout
    oldStderr= sys.stderr
    stdout= FileLikeList()
    stderr= FileLikeList()
    sys.stdout= stdout
    sys.stderr= stderr
    
    from urllib import unquote
    
    #~ try:
    params= {}
    for param in environ['QUERY_STRING'].split('&'):
        blah= param.split('=')
        params[blah[0]]= unquote(blah[1])
    
    import tlgbackend
    
    action= params['action']
    if action=='query':
        tlgbackend.TaskListGenerator().run(wiki=params['lang']+'wiki', queryString=params['query'], queryDepth=params['querydepth'], flaws=params['flaws'])
        #~ tlgbackend.TaskListGenerator().run(wiki='dewiki', queryString='Biologie -Meerkatzenverwandte -Astrobiologie', queryDepth=2, flaws='MissingSourcesTemplates Unlucky')
    elif action=='listflaws':
        tlgbackend.TaskListGenerator().listFlaws()
    
    sys.stdout= oldStdout
    sys.stderr= oldStderr
    start_response('200 OK', [('Content-Type', 'text/plain')])
    
    #~ for i in params:
        #~ stdout.values.append("%s = %s\n" % (i, params[i]))
    
    return stdout.values
    #~ return params  #sys.stdout.values

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
