#!/usr/bin/python
# task list generator - wsgi interface to the backend
import tlgbackend

def myapp(environ, start_response):
    start_response('200 OK', [('Content-Type', 'text/plain')])
    return ['Hello World!\n']

if __name__ == "__main__":
    # change this to "flup.server.scgi" when/if fcgid is installed on the toolserver
    from flup.server.cgi import WSGIServer
    import cgitb
    cgitb.enable()
    WSGIServer(myapp).run()
