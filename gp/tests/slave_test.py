#!/usr/bin/python
# -*- coding: utf-8

import unittest
import tempfile
from test_base import *
from gp.client import *


def read_lines( filepath ):
    f = open(filepath, 'ra')
    lines = [ s for s in f ]
    
    f.close()
    return lines


# Tests a connection to a slave process, as well as general client
# lib functionality.

class SlaveTest (SlaveTestBase, unittest.TestCase):
    """Client Lib Functions

    Tested here, not in ConnectionTestBase, because we only need to test
    is once, not for every type of connection
    
    @TODO: test getStatusMessage, isClosed, etc
    
    """
    
    def test_try(self):
        x = self.gp.try_foo()
        self.assertFalse(x)
        self.assertEquals('FAILED', self.gp.getStatus())
    
    def test_capture(self):
        """empty data"""
        a = self.gp.capture_list_roots()
        self.assertStatus( 'OK' )
        self.assertNotNone( a )
        self.assertIsInstance(a, (list,tuple))
        
        self.assertEquals( 0, len(a),
          "number of items in the result should be 0!" )
        
        # single column data
        self.gp.add_arcs(((1, 11 ),(1, 12 ), (11, 111 ), (11, 112 ),))        
        a = self.gp.capture_list_successors( 1 )
        self.assertStatus( 'OK' )
        self.assertNotNone( a )
        self.assertIsInstance(a, (list,tuple))
        
        a = array_column( a, 0 )
        self.assertEquals( [11, 12], a,
          "unexpected response for list_successors(1): %s" % a )
        
        # two column data
        a = self.gp.capture_stats()
        self.assertNotNone( a )
        
        a = pairs2map( a )
        self.assertContains( 'ArcCount', a,
          "contents: %s" % a )
        self.assertEquals( a['ArcCount'], 4 )
        
        # two column data as map
        a = self.gp.capture_stats_map()
        self.assertContains( 'ArcCount', a,
          "contents: %s" % a )
        self.assertEquals( a['ArcCount'], 4 )
        
        # capture none
        a = self.gp.capture_traverse_successors( 77, 5 )
        self.assertStatus( 'NONE' )
        self.assertNone( a )
        
        # capture on command with no output
        a = self.gp.capture_clear()
        self.assertStatus( 'OK' )
        self.assertTrue( a )
        
        # capture throwing error
        try:  
            x = self.gp.capture_foo()
            self.fail("capturing output of an unknown command should "
              + "trigger an error")
        except gpProcessorException, e:
            # this is the expected outcome: the connection is closed.
            pass
        
        # capture with try
        x = self.gp.try_capture_foo()
        # should not trigger an exception...
         
    def dummyCallHandler(self, gp, params):
        if ( params['command'] == 'dummy' ):  
            params['result'] = "test"
            return False
        return True
         
    
    def test_callHandler(self):
        h = self.dummyCallHandler
        self.gp.addCallHandler(h)
        
        st = self.gp.capture_stats()
        
        if st == False:
            self.fail("capture_stats failed and returned False")
        elif st == True:
            self.fail("capture_stats failed to capture, returned True")
        
        self.assertTrue(type(st) in (list, tuple),
          'capture_stats is expected to return a list, got %s!' % type(st))
        
        x = self.gp.dummy()
        self.assertEquals('test', x)
         
    
    def assertCommandAccepted(self, cmd, src=None, sink=None):
        s = re.sub('/\s+/s', ' ', str( cmd ))
        
        try:
            x = self.gp.execute(cmd, src, sink)
            raise Exception(
              "dummy command should have failed in core: %s" % s)
        except gpUsageException, e:
            self.fail("command syntax should be accepted by client: %s" % s)
        except gpProcessorException, e:
            # ok, should fail in core, but be accepted by client side validator
            pass
             
    def assertCommandRejected(self, cmd, src=None, sink=None):
        s = re.sub('/\s+/s', ' ', str(cmd))
        
        try:
            x = self.gp.execute(cmd, src, sink)
            self.fail("bad command should be detected: %s" % s)
        except gpUsageException, e:
            pass # ok
        except gpProcessorException, e:
            self.fail(
              ( "bad command should have been detected by the client: %s"
              + "; core message: %s" ) % (s, e.getMessage()) )
    
    def test_commandValidation(self):
        self.assertCommandRejected( '' )
        self.assertCommandRejected(('', 23) )
        
        self.assertCommandRejected( None )
        self.assertCommandRejected((None, 23) )
        
        self.assertCommandRejected( False )
        self.assertCommandRejected((False, 23) )
        
        self.assertCommandRejected(() )
        self.assertCommandRejected((('foo',), ) )
        
        self.assertCommandRejected( '123' )
        self.assertCommandRejected(('123',) )
        
        self.assertCommandAccepted( ' x ' )
        self.assertCommandRejected((' x ',) )
        
        self.assertCommandRejected( '<x>y' )
        self.assertCommandRejected((' <x>y ',) )
        
        self.assertCommandRejected(('a:b') )
        # 'a:b' is legal as an argument, but nut as a command name!
        
        self.assertCommandAccepted( 'x' )
        self.assertCommandAccepted(('x',) )
        
        self.assertCommandAccepted( 'xyz' )
        self.assertCommandAccepted(('xyz',) )
        
        self.assertCommandAccepted( 'x7y' )
        self.assertCommandAccepted(('x7y',) )
        
        chars = "\r\n\t\0\x09^\"§\$%/()[]\{\}=?'`\\*+~.,;@\xDD"
        for ch in chars:
            s = "a " + ch + " b"
            
            self.assertCommandRejected( s )
            self.assertCommandRejected((s,) )
             
        
        chars = " !&<>|#:"
        for ch in chars:
            s = "a " + ch + " b"
            
            self.assertCommandRejected((s,) )
             
        
        # operators -----------------------------------------
        self.assertCommandAccepted( 'clear && clear' )
        self.assertCommandAccepted( 'clear !&& clear' )
        
        
        # pipes disallowed -----------------------------------------
        self.gp.allowPipes = False
        
        self.assertCommandRejected( 'clear > /tmp/test' )
        self.assertCommandRejected( 'clear < /tmp/test' )
        
        # pipes allowed -----------------------------------------
        self.gp.allowPipes = True
        
        self.assertCommandAccepted( 'clear > /tmp/test' )
        self.assertCommandAccepted( 'clear < /tmp/test' )
        
        # pipes conflict -----------------------------------------
        self.assertCommandRejected( 'clear > /tmp/test', None,
          NullSink.instance )
        self.assertCommandRejected( 'clear < /tmp/test',
          NullSource.instance, None )
         
    
    def assertArgumentAccepted(self, arg ):
        s = re.sub('/\s+/s', ' ', str(arg))
        
        try:
            x = self.gp.execute(('foo', arg) )
            raise Exception("dummy command should have failed in core: "
              + "foo %s" % s)
        except gpUsageException, e:
            self.fail("argument should be accepted by client: %s" % s)
        except gpProcessorException, e:
            pass
            # ok, should fail in core, but be accepted by client side
            # validator

    def assertArgumentRejected( self, arg ):
        s = re.sub('/\s+/s', ' ', str(arg))
        try:
            x = self.gp.execute(('foo', arg) )
            self.fail("malformed argument should be detected: %s" % s)
        except gpUsageException, e:
            pass
            # ok
        except gpProcessorException, e:
            self.fail("malformed argument should have been detected "
              + "by the client: %s; core message: %s"
              % (s, e.getMessage()) )              
         
    
    def test_argumentValidation(self):
        self.assertArgumentRejected( '' )
        self.assertArgumentRejected( None )
        self.assertArgumentRejected( False )
        self.assertArgumentRejected( ' x ' )
        
        # self.gp.setTimeout(2); # has no effect for pipes
        self.assertArgumentAccepted( 'x:y' )
        # needed for password auth! 
        # NOTE: This is broken in graphcore (but works via graphserv)!
        
        self.assertArgumentAccepted( '123' )
        self.assertArgumentAccepted( 'x' )
        self.assertArgumentAccepted( 'xyz' )
        self.assertArgumentAccepted( 'x7y' )
        self.assertArgumentAccepted( '7x7' )
        
        chars = " \r\n\t\0\x09^!\"§\$%&/()[]\ \ =?'#`\\*+~., ;<>|@\xDD"
        for ch in chars:
            s = "a " + ch + " b"
            
            self.assertArgumentRejected(s)
             
         
    
    # // Client Lib I/O ///////////////////////////////////////////////////////////////
    # Tested here, not in ConnectionTestBase, because we only need to test is once, not for every type of connection
    # Note: ArraySource and ArraySink are used implicitly all the time in the tests, no need to test them separately.
    
    def test_fileSource(self):
        f = os.path.dirname(os.path.abspath(__file__)) + '/gp.test.data'  
        src = FileSource(f)
        
        self.gp.add_arcs( src )
        
        self.assertStatus( 'OK' )
        self.assertStatsValue( 'ArcCount', 4 )
        
        arcs = self.gp.capture_list_successors( 1 )
        
        self.assertTrue( ConnectionTestBase.setEquals(
          arcs, [(11, ), (12, ),]), "sucessors of (1): expected [(11,), (12,)], got %s" % arcs )
        
        arcs = self.gp.capture_list_successors( 11 )
        self.assertTrue( ConnectionTestBase.setEquals(
          arcs, [(111, ), (112, ),]), "sucessors of (2): expected [(111,), (112,)], got %s" % arcs )
         
    
    def test_fileSink(self):
        
        # set up the sink
        f = tempfile.mktemp(suffix='gpt')
        sink = FileSink(f, False, "\n")
        
        # generate output
        self.gp.add_arcs(((1, 11 ), (1, 12 ), (11, 111 ), (11, 112 ),))
        
        ok = self.gp.traverse_successors(1, 2, sink)
        sink.close()
        
        # make sure we can read the file
        self.assertStatus('OK')
        self.assertEquals('OK', ok)
        
        # compare actual file contents
        rows = read_lines(f)
        
        self.assertNotEquals(False, rows,
          "could not get file contents of %s" % f)
        self.assertNotNone(rows, "could not get file contents of %s " % f)
        
        expected =("1\n", "11\n", "12\n", "111\n", "112\n",)
        self.assertTrue( ConnectionTestBase.setEquals(
          expected, rows), 'bad content in outfile: %s, expected %s' 
                                % ( rows, expected ) )
        
        #cleanup
        try:
            unlink(f)
        except:
            pass
    
    def test_nullSource(self):
        self.gp.add_arcs( NullSource.instance )
        self.assertStatus( 'OK' )
         
    
    def test_nullSink(self):
        # generate output
        self.gp.add_arcs(((1, 11 ), (1, 12 ), (11, 111 ), (11, 112 ),))
        
        ok = self.gp.traverse_successors(1, 2, NullSink.instance)
        self.assertStatus('OK')
         
    
    # //// Slave Connection Tests ///////////////////////////////////////////////////
    # currently none. could check if the process really dies after quit, etc
    # TODO: test checkPeer, etc
     
if __name__ == '__main__':
    unittest.main()
