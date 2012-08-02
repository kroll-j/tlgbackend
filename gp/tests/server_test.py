#!/usr/bin/python
# -*- coding: utf-8

import unittest
import os
import tempfile
from test_base import *
from gp.client import *

test_graph_name = 'test' + str(os.getpid())
TestFilePrefix = '/tmp/gptest-' + str(os.getpid())


class ServerTest (ClientTestBase, unittest.TestCase):
    """Test server functions via client lib."""

    def test_createGraph(self):
        """Graph management functions"""

        global test_graph_name

        name = test_graph_name + "_2"

        # create the graph
        self.gp.create_graph(name)

        #make sure we can't create it twice
        ok = self.gp.try_create_graph(name)
        self.assertFalse( ok, "should not be able to create graph again when it already exists" )

        # see if we can use the graph from another connection
        gp2 = self.newConnection()

        gp2.use_graph(name)

        # see if we can drop the graph while it's used
        self.gp.drop_graph(name)

        #TODO: gp2 should now report errors, because the grpah is gone. test that.

        # see if we can re-create the graph after it was dropped
        self.gp.create_graph(name)
        self.gp.drop_graph(name)

        #TODO: test name restrictions


    def test_createNameRestrictions(self):
        global test_graph_name

        self.gp.strictArguments = False
        # disable strict client-side validation

        try:
            n = ''
            ok = self.gp.create_graph(n)
            self.fail("empty graph names should be forbidden!" )
        except gpException, ex:
            pass
            # ok


        n = '1337'
        ok = self.gp.try_create_graph(n)
        self.assertFalse(ok, "numeric graph names should be forbidden! (name: `" + n + "`)" )

        n = '1337' + test_graph_name
        ok = self.gp.try_create_graph(n)
        self.assertFalse( ok,
          "graph names starting with a number should be forbidden! (name: `"
          + n + "`)" )

        chars = " \r\n\t\0\x09^!\"§\$%&/()[]\ \ =?'#`\\*+~.:, ;<>|@"
        for ch in chars:
            try:
                n = test_graph_name + ch + "test"
                ok = self.gp.create_graph(n)
                self.fail("graph names containing `"
                  + ch + "` should be forbidden! (name: `"
                  + n + "`)" )
            except gpException, ex:
                pass
                # ok
            try:
                n = ch + test_graph_name
                ok = self.gp.create_graph(n)
                self.fail("graph names starting with  `"
                  + ch + "` should be forbidden! (name: `" + n + "`)")
            except gpException, ex:
                pass
                # ok

        n = 'test1337' + test_graph_name
        ok = self.gp.try_create_graph(n)
        self.assertEquals( 'OK', ok,
          "graph names containing numbers should be allowd! (name: `"
          + n+ "`)")
        self.gp.try_drop_graph(n)

        chars = '-_8'
        for ch in chars:
            n = 'test' + ch + test_graph_name
            ok = self.gp.try_create_graph(n)
            self.assertEquals( 'OK', ok, "graph names containing `"
              + ch + "` should be allowd! (name: `" + n + "`)")
            self.gp.try_drop_graph(n)

    def test_dropGraph(self):
        global test_graph_name

        name = test_graph_name + "_2"

        self.gp.create_graph(name)
        self.gp.drop_graph(name)

        ok = self.gp.try_use_graph(name)
        self.assertFalse( ok,
          "should not be able to use graph after dropping it" )
        ok = self.gp.try_drop_graph(name)
        self.assertEquals( 'NONE', ok, "should not be able to drop "
          + "graph again after it was already dropped." )

    def test_listGraphs(self):
        global test_graph_name

        gp2 = self.newConnection()
        graphs = gp2.capture_list_graphs()
        graphs = array_column(graphs, 0)
        self.assertTrue( test_graph_name in graphs,
          "test table test_graph_name should be in the list" )

        self.gp.drop_graph(test_graph_name)
        graphs = gp2.capture_list_graphs()
        #print "graphs: " . var_export($graphs, true) . "\n"

        graphs = array_column( graphs, 0 )

        #print "graphs: " . var_export($graphs, true) . "\n"

        #print "containes: " . var_export(ConnectionTestBase::setContains( $graphs, test_graph_name ), true) . "\n"

        self.assertFalse(
          ConnectionTestBase.setContains(graphs, test_graph_name),
          "test table test_graph_name should no longer be in the list" )

    def test_shutdown(self):
        global test_graph_name

        gp2 = self.newConnection()
        gp2.use_graph(test_graph_name)
        gp2.stats()

        self.assertSessionValue('ConnectedGraph', test_graph_name)

        self.gp.shutdown() # <------------------
        # self.assertSessionValue('ConnectedGraph', 'None');
        # nice, but not reliable. race condition.

        self.gp.try_stats()
        self.assertEquals( 'FAILED', self.gp.getStatus(),
          'fetching stats should fail after shutdown' )

        gp2.try_stats()
        self.assertEquals( 'FAILED', gp2.getStatus(),
          'fetching stats should fail after shutdown' )
        gp2.close()

        gp3 = self.newConnection()
        gp3.try_use_graph(test_graph_name)
        self.assertEquals( 'FAILED', gp3.getStatus(),
          'graph should be unavailable after shutdown' )
        gp3.close()


    def test_quit(self):
        global test_graph_name

        gp2 = self.newConnection()
        gp2.use_graph(test_graph_name)
        gp2.stats()

        self.assertSessionValue('ConnectedGraph', test_graph_name)

        self.gp.quit()  # <------------------
        self.assertStatus('OK')

        try:
            self.gp.try_stats()
            self.fail( 'connection should be unusable after quit' )
        except gpProtocolException, e:
            pass
            # ok


        gp2.stats()
        self.assertEquals( 'OK', gp2.getStatus(),
          'connection should still be usable by others after quit; response: %s' % gp2.getResponse() )
        gp2.close()

        gp3 = self.newConnection()
        gp3.use_graph(test_graph_name)
        self.assertEquals( 'OK', gp3.getStatus(),
          'graph should still be available to others after quit; response: %s' % gp2.getResponse() )
        gp3.close()


    # privileges
    def test_createGraphPrivilege(self):
        global test_graph_name
        global test_admin, test_admin_password
        global test_master, test_master_password

        name = test_graph_name + "_2"

        gp = self.newConnection()

        ok = gp.try_create_graph(name)
        self.assertFalse( ok,
          "should not be able to create a graph without authorizing" )

        gp.authorize('password',
          test_master + ":" + test_master_password)
        ok = gp.try_create_graph(name)
        self.assertFalse( ok,
          "should not be able to create a graph without admin privileges" )

        gp.authorize('password',
          test_admin + ":" + test_admin_password)
        # re-authenticate
        ok = gp.create_graph(name)
        self.assertEquals( ok, 'OK',
          "should be able to create graph with admin privileges; response: %s" % gp.getResponse() )

        gp.try_drop_graph(name)
        # cleanup


    def test_dropGraphPrivilege(self):
        global test_graph_name
        global test_admin, test_admin_password
        global test_master, test_master_password

        name = test_graph_name

        gp = self.newConnection()

        ok = gp.try_drop_graph(name)
        self.assertFalse( ok, "should not be able to drop a graph without authorizing" )

        gp.authorize('password',
          test_master + ":" + test_master_password)
        ok = gp.try_drop_graph(name)
        self.assertFalse( ok,
          "should not be able to drop a graph without admin privileges" )

        gp.authorize('password',
          test_admin + ":" + test_admin_password)
        # re-authenticate
        ok = gp.drop_graph(name)
        self.assertEquals( ok, 'OK',
          "should be able to drop graph with admin privileges; response: %s" % gp.getResponse() )

    def test_inputPipingPrivilege(self):
        global test_graph_name, test_graphserv_host
        global test_admin, test_admin_password
        global test_master, test_master_password

        #XXX: this uses local files, so it will always fail
        #     if the server isn't on localhost!
        if test_graphserv_host != 'localhost':
            return None

        f = os.path.dirname(os.path.abspath(__file__)) + '/gp.test.data'

        gp = self.newConnection()
        gp.use_graph(test_graph_name)
        gp.allowPipes = True

        gp.authorize('password',
          test_master + ":" + test_master_password)

        try:
            ok = gp.execute("add-arcs < " + f)
            self.fail(
              "should not be able to pipe without admin privileges!" )
        except gpProcessorException, ex:
            self.assertEquals( 'DENIED', gp.getStatus(),
              "piping should be denied, not fail. Message: "
              + str(ex))


        gp.authorize('password', test_admin + ":" + test_admin_password)
        # re-authenticate
        ok = gp.execute("add-arcs < " + f)
        self.assertEquals( ok, 'OK',
          "should be able to pipe with admin privileges; response: %s" % gp.getResponse() )


    def test_outputPipingPrivilege(self):
        global test_graph_name, test_graphserv_host
        global test_admin, test_admin_password
        global test_master, test_master_password

        #XXX: this uses local files, so it will always fail
        #     if the server isn't on localhost!
        if test_graphserv_host != 'localhost':
            return None

        f = tempfile.mktemp(suffix='gpt')

        gp = self.newConnection()
        gp.use_graph(test_graph_name)
        gp.allowPipes = True

        try:
            ok = gp.execute("list-roots > " + f)
            self.fail(
              "should not be able to pipe without admin privileges!" )
        except gpProcessorException, ex:
            self.assertEquals( 'DENIED', gp.getStatus(),
              "piping should be denied, not fail. Message: "
              + str(ex))

        gp.authorize(
          'password', test_admin + ":" + test_admin_password)
        # re-authenticate
        ok = gp.execute("list-roots > " + f)
        self.assertEquals(
          ok, 'OK', "should be able to pipe with admin privileges; response: %s" % gp.getResponse() )

        try:
            unlink(f)
            # cleanup
        except:
            pass

    def test_addArcsPrivilege(self):
        global test_graph_name
        global test_master, test_master_password

        gp = self.newConnection()
        gp.use_graph(test_graph_name)

        ok = gp.try_add_arcs(((1, 11 ), (1, 12 ) ) )
        self.assertFalse(
          ok, "should not be able to add arcs without authorizing" )
        self.assertEquals('DENIED', gp.getStatus(),
          "command should be denied, not fail" )

        gp.authorize('password',
          test_master + ":" + test_master_password)
        ok = gp.try_add_arcs(((1, 11 ), (1, 12 ) ) )
        self.assertEquals( 'OK', ok,
          "should be able to add arcs with updater privileges; response: %s" % gp.getResponse() )

    def test_removeArcsPrivilege(self):
        global test_graph_name
        global test_master, test_master_password

        self.gp.add_arcs(((1, 11 ), (1, 12 ) ) )
        # add some arcs as admin

        gp = self.newConnection()
        gp.use_graph(test_graph_name)

        ok = gp.try_remove_arcs(((1, 11 ), ) )
        self.assertFalse( ok,
          "should not be able to delete arcs without authorizing" )
        self.assertEquals( 'DENIED', gp.getStatus(),
          "command should be denied, not fail" )

        gp.authorize('password',
          test_master + ":" + test_master_password)
          
        ok = gp.try_remove_arcs(((1, 11 ), ) )
        self.assertEquals( 'OK', ok,
          "should be able to delete arcs with updater privileges; response: %s" % gp.getResponse() )

    def test_replaceSuccessorsPrivilege(self):
        global test_graph_name
        global test_master, test_master_password

        self.gp.add_arcs(((1, 11 ), (1, 12 ) ) )
        # add some arcs as admin

        gp = self.newConnection()
        gp.use_graph(test_graph_name)

        ok = gp.try_replace_successors( 1, (17, ) )
        self.assertFalse( ok,
          "should not be able to replace arcs without authorizing" )
        self.assertEquals( 'DENIED', gp.getStatus(),
          "command should be denied, not fail" )

        gp.authorize('password',
          test_master + ":" + test_master_password)
        ok = gp.try_replace_successors( 1, (17, ) )
        self.assertEquals( 'OK', ok,
          "should be able to replace arcs with updater privileges; response: %s" % gp.getResponse() )

    def test_replacePredecessorsPrivilege(self):
        global test_graph_name
        global test_master, test_master_password

        self.gp.add_arcs(((1, 11 ), (1, 12 ) ) )
        # add some arcs as admin

        gp = self.newConnection()
        gp.use_graph(test_graph_name)

        ok = gp.try_replace_predecessors( 1, (17, ) )
        self.assertFalse( ok,
          "should not be able to replace arcs without authorizing" )
        self.assertEquals( 'DENIED', gp.getStatus(),
          "command should be denied, not fail" )

        gp.authorize('password',
          test_master + ":" + test_master_password)
        ok = gp.try_replace_predecessors( 1, (17, ) )
        self.assertEquals( 'OK', ok,
          "should be able to replace arcs with updater privileges; response: %s" % gp.getResponse() )

    def testClearPrivilege(self):
        global test_graph_name
        global test_admin, test_admin_password
        global test_master, test_master_password

        gp = self.newConnection()
        gp.use_graph(test_graph_name)

        ok = gp.try_clear()
        self.assertFalse( ok,
          "should not be able to clear a graph without authorizing" )

        gp.authorize('password',
          test_master + ":" + test_master_password)
        ok = gp.try_clear()
        self.assertEquals( ok, 'OK',
          "should be able to clear graph with updater privileges" )

        gp.authorize('password',
          test_admin + ":" + test_admin_password)
        # re-authenticate
        ok = gp.try_clear()
        self.assertEquals( ok, 'OK',
          "should be able to clear graph with admin privileges" )

    def test_shutdownPrivilege(self):
        global test_graph_name
        global test_admin, test_admin_password
        global test_master, test_master_password

        gp = self.newConnection()
        gp.use_graph(test_graph_name)

        ok = gp.try_shutdown()
        self.assertFalse( ok,
          "should not be able to shut down a graph without authorizing" )

        gp.authorize('password',
          test_master + ":" + test_master_password)
        ok = gp.try_shutdown()
        self.assertFalse( ok, "should not be able to shut down a graph "
          + "without admin privileges" )

        gp.authorize('password',
          test_admin + ":" + test_admin_password)
        # re-authenticate
        ok = gp.try_shutdown()
        self.assertEquals( ok, 'OK',
          "should be able to shut down graph with admin privileges" )


    def test_traverseSuccessorsWithout(self):
        self.gp.add_arcs( [
            ( 1, 11 ),
            ( 1, 12 ),
            ( 11, 111 ),
            ( 11, 112 ),
            ( 111, 1111 ),
            ( 111, 1112 ),
            ( 112, 1121 ),
        ] )
        
        self.assertStatsValue( 'ArcCount', 7 )
        
        #--------------------------------------------
        succ = self.gp.capture_traverse_successors_without( 11, 5, 111, 5 )

        self.assertEquals( [ (11,), (112,), (1121,), ], succ )
    



#TODO: (optionally) start server instance here! let it die when the test script dies.

#TODO: CLI interface behaviour of server (port config, etc)

if __name__ == '__main__':
	unittest.main()
