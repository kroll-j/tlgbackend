from test_base import *
from gp.client import *
from gp.mysql import *
from gp.mysql import _fetch_dict

import unittest
import sys

class MySQLTest (SlaveTestBase, unittest.TestCase):
    mysql = None
    
    def setUp(self):
        self.dump = PipeSink( sys.stdout )

        try:
            self.gp = MySQLGlue.new_slave_connection( test_graphcore_path )
            self.gp.connect()
        except gpException as ex:
            print "Unable to launch graphcore instance from %s, please make sure graphcore is installed and check the test_graphcore_path configuration options in test_config.py.\nOriginal error: %s " % (test_graphcore_path, ex.getMessage() )
            suicide(10)
        
        try: 
            self.gp.mysql_connect(test_mysql_host, test_mysql_user, test_mysql_password, test_mysql_database)
        except gpException as ex:
            print "Unable to connect to database %s on MySQL host %s as %s, please make sure MySQL is running and check the test_mysql_host and related configuration options in test_cofig.py.\nOriginal error: %s " % (test_mysql_database, test_mysql_host, test_mysql_user, ex.getMessage() )
            suicide(10)
    
    def _make_table( self, table, fieldSpec ):
        sql = "CREATE TEMPORARY TABLE IF NOT EXISTS " + table
        sql += "("
        sql += fieldSpec
        sql += ")"
        
        self.gp.mysql_query(sql)
        
        sql = "TRUNCATE TABLE " + table
        self.gp.mysql_query(sql)
    
    
    def test_Source(self):
        self._make_table( "test", "a INT NOT NULL, b INT NOT NULL" )
        self.gp.mysql_query( "INSERT INTO test VALUES (3, 8)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (7, 9)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (11, 11)" )
        
        #-----------------------------------------------------------
        src = self.gp.make_source( MySQLTable("test", "a", "b") )
        
        self.assertEquals(( 3, 8 ), src.next() , "expected row to be 3,8 " )
        self.assertEquals(( 7, 9 ), src.next() , "expected row to be 7,9" )
        self.assertEquals(( 11, 11 ), src.next() , "expected row to be 11,11" )
        
        try:
            r = src.next()
            self.fail( "expected no more rows, got %s " % (r, ) )
        except StopIteration:
            pass
        
        src.close()

        #-----------------------------------------------------------
        src = self.gp.make_source( MySQLSelect("select a from test where a > 7") )
        
        self.assertEquals((11,), src.next() , "expected row to be 11" )
        
        try:
            r = src.next()
            self.fail( "expected no more rows, got %s " % (r, ) )
        except StopIteration:
            pass
        
        src.close()
    

    def test_SelectInto(self):
        self._make_table( "test", "a INT NOT NULL, b INT NOT NULL" )
        self.gp.mysql_query( "INSERT INTO test VALUES (3, 8)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (7, 9)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (11, 11)" )
        
        #-----------------------------------------------------------
        sink = ArraySink()
        self.gp.select_into( "select a, b from test order by a, b", sink )

        data = sink.getData()
        
        self.assertEquals([( 3, 8 ), ( 7, 9 ), ( 11, 11 )], data )
    

    def test_UnbufferedSelectInto(self):
        self._make_table( "test", "a INT NOT NULL, b INT NOT NULL" )
        self.gp.set_unbuffered(True)
        self.gp.mysql_query( "INSERT INTO test VALUES (3, 8)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (7, 9)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (11, 11)" )
        
        #-----------------------------------------------------------
        sink = ArraySink()
        self.gp.select_into( "select a, b from test order by a, b", sink )

        data = sink.getData()
        
        self.assertEquals( [ ( 3, 8 ), ( 7, 9 ), ( 11, 11 ) ], data )
    
    def assertNextRowEquals(self, expected, res):
        row = _fetch_dict(res)
        self.assertTrue( TestBase.dictEquals(expected, row), "expected row to be %s, got %s" % (expected, row) )
        
    def test_TempSink(self):
        snk = self.gp.make_temp_sink( MySQLTable("?", "a", "b") )
        table = snk.getTable()
        
        snk.putRow( (4,5) )
        snk.putRow( (6,7) )
        snk.close()
        
        res = self.gp.mysql_query( "SELECT a, b FROM " + table.get_name() + " ORDER BY a, b")
        
        self.assertNextRowEquals({ 'a': 4, 'b': 5 }, res )
        self.assertNextRowEquals({ 'a': 6, 'b': 7 }, res )
        self.assertFalse( res.fetchone(), "expected next row to be false" )
        
        res.close()
        
        snk.drop()
    

    def test_AddArcsFromSourceObject(self):
        self._make_table( "test", "a INT NOT NULL, b INT NOT NULL" )
        self.gp.mysql_query( "INSERT INTO test VALUES (1, 11)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (1, 12)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (11, 111)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (11, 112)" )
        
        #-----------------------------------------------------------
        src = self.gp.make_source( MySQLTable("test", "a", "b") )
        self.gp.add_arcs( src )
        src.close()
        
        self.assertStatus( 'OK' )
        self.assertStatsValue( 'ArcCount', 4 )
        
        arcs = self.gp.capture_list_successors( 1 );        
        self.assertTrue( ConnectionTestBase.setEquals( arcs, [
            ( 11, ),
            ( 12, ),
        ] ), "sucessors of (1)" )
        
        arcs = self.gp.capture_list_successors( 11 )
        self.assertTrue( ConnectionTestBase.setEquals( arcs, [
            ( 111, ),
            ( 112, ),
        ] ), "sucessors of (2)" )
    

    def test_AddArcsFromSourceShorthand(self):
        self._make_table( "test", "a INT NOT NULL, b INT NOT NULL" )
        self.gp.mysql_query( "INSERT INTO test VALUES (1, 11)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (1, 12)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (11, 111)" )
        self.gp.mysql_query( "INSERT INTO test VALUES (11, 112)" )
        
        #-----------------------------------------------------------
        src = self.gp.add_arcs_from( "test a b" )
        src.close()
        
        self.assertStatus( 'OK' )
        self.assertStatsValue( 'ArcCount', 4 )
        
        arcs = self.gp.capture_list_successors( 1 );        
        self.assertTrue( ConnectionTestBase.setEquals( arcs, [
            ( 11, ),
            ( 12, ),
        ] ), "sucessors of (1)" )
        
        arcs = self.gp.capture_list_successors( 11 )
        self.assertTrue( ConnectionTestBase.setEquals( arcs, [
            ( 111, ),
            ( 112, ),
        ] ), "sucessors of (2)" )
        
        #-----------------------------------------------------------
        self.gp.clear()
        stats = self.gp.capture_stats_map()
        self.assertEquals( 0, stats['ArcCount'], "ArcCount" )

        #self.gp.setDebug(True)
        src = self.gp.add_arcs_from( "select a, b from test" )
        src.close()
        
        stats = self.gp.capture_stats_map()
        self.assertEquals( 4, stats['ArcCount'], "ArcCount" )
        
        #-----------------------------------------------------------
        self.gp.clear()

        src = self.gp.add_arcs_from( ("test", "a", "b") )
        src.close()
        
        self.assertStatsValue( 'ArcCount', 4 )

        #-----------------------------------------------------------
        self.gp.clear()

        src = self.gp.add_arcs_from( MySQLTable("test", "a", "b") )
        src.close()
        
        self.assertStatsValue( 'ArcCount', 4 )
    

    def testSuccessorsToSinkObject(self):
        self.gp.add_arcs( [
            (  1, 11  ),
            (  1, 12  ),
            (  11, 111  ),
            (  11, 112  ),
        ] )
        
        #-----------------------------------------------------------
        snk = self.gp.make_temp_sink( MySQLTable("?", "n") )
        src = self.gp.traverse_successors( 1, 8, snk )
        snk.close()
        table = snk.getTable()
        
        res = self.gp.mysql_query( "SELECT n FROM "+table.get_name()+" ORDER BY n")
        
        self.assertNextRowEquals({ 'n': 1L }, res )
        self.assertNextRowEquals({ 'n': 11L }, res )
        self.assertNextRowEquals({ 'n': 12L }, res )
        self.assertNextRowEquals({ 'n': 111L }, res )
        self.assertNextRowEquals({ 'n': 112L }, res )
        self.assertFalse(  res.fetchone(), "expected next row to be False" )
        
        res.close()
        
        #-----------------------------------------------------------
        self.gp.set_max_allowed_packet(6); #force inserter to flush intermittedly
        
        snk = self.gp.make_temp_sink( MySQLTable("?", "n") )
        src = self.gp.traverse_successors( 1, 8, snk )
        snk.close()
        table = snk.getTable()
        
        res = self.gp.mysql_query( "SELECT n FROM "+table.get_name()+" ORDER BY n")
        
        self.assertNextRowEquals({ 'n': 1 }, res )
        self.assertNextRowEquals({ 'n': 11 }, res )
        self.assertNextRowEquals({ 'n': 12 }, res )
        self.assertNextRowEquals({ 'n': 111 }, res )
        self.assertNextRowEquals({ 'n': 112 }, res )
        self.assertFalse(  res.fetchone(), "expected next row to be False" )
        
        res.close()
    

    def test_SuccessorsToSinkShorthand(self):
        self.gp.add_arcs( [
            (  1, 11  ),
            (  1, 12  ),
            (  11, 111  ),
            (  11, 112  ),
        ] )
        
        #-----------------------------------------------------------
        snk = self.gp.traverse_successors_into( 1, 8, "? n" )
        snk.close()
        table = snk.getTable()
        
        res = self.gp.mysql_query( "SELECT n FROM "+table.get_name()+" ORDER BY n")
        
        self.assertNextRowEquals({ 'n': 1 }, res )
        self.assertNextRowEquals({ 'n': 11 }, res )
        self.assertNextRowEquals({ 'n': 12 }, res )
        self.assertNextRowEquals({ 'n': 111 }, res )
        self.assertNextRowEquals({ 'n': 112 }, res )
        self.assertFalse(  res.fetchone(), "expected next row to be False" )
        
        res.close()
        snk.drop()

        #---------------------------------------------------------
        snk = self.gp.traverse_successors_into( 1, 8, ( "?", "n" ) )
        snk.close()
        table = snk.getTable()
        
        res = self.gp.mysql_query( "SELECT n FROM "+table.get_name()+" ORDER BY n")
        
        self.assertNextRowEquals({ 'n': 1 }, res )
        self.assertNextRowEquals({ 'n': 11 }, res )
        self.assertNextRowEquals({ 'n': 12 }, res )
        self.assertNextRowEquals({ 'n': 111 }, res )
        self.assertNextRowEquals({ 'n': 112 }, res )
        self.assertFalse(  res.fetchone(), "expected next row to be False" )
        
        res.close()
        snk.drop()

        #---------------------------------------------------------
        snk = self.gp.traverse_successors_into( 1, 8, MySQLTable("?", "n") )
        snk.close()
        table = snk.getTable()
        
        res = self.gp.mysql_query( "SELECT n FROM "+table.get_name()+" ORDER BY n")
        
        self.assertNextRowEquals({ 'n': 1 }, res )
        self.assertNextRowEquals({ 'n': 11 }, res )
        self.assertNextRowEquals({ 'n': 12 }, res )
        self.assertNextRowEquals({ 'n': 111 }, res )
        self.assertNextRowEquals({ 'n': 112 }, res )
        self.assertFalse(  res.fetchone(), "expected next row to be False" )
        
        res.close()
        snk.drop()

        #---------------------------------------------------------
        self._make_table( "test_n", "n INT NOT NULL" )

        table = MySQLTable("test_n", "n")
        snk = self.gp.traverse_successors_into( 1, 8, table )
        snk.close()
        
        res = self.gp.mysql_query( "SELECT n FROM "+table.get_name()+" ORDER BY n")
        
        self.assertNextRowEquals({ 'n': 1 }, res )
        self.assertNextRowEquals({ 'n': 11 }, res )
        self.assertNextRowEquals({ 'n': 12 }, res )
        self.assertNextRowEquals({ 'n': 111 }, res )
        self.assertNextRowEquals({ 'n': 112 }, res )
        self.assertFalse(  res.fetchone(), "expected next row to be False" )
        
        res.close()
    
if __name__ == '__main__':
    unittest.main()
