#!/usr/bin/python
# -*- coding: utf-8

import unittest
import os
import tempfile
from test_base import *
from test_config import *
from gp.client import *
 
class CoreTest (SlaveTestBase, unittest.TestCase):
    """
        Tests core functions via client lib
    """
    
    #### Core Functions ###############################/
    
    def test_addArcs(self):
        self.gp.add_arcs( (
            ( 1, 11 ),
            ( 1, 12 ),
            ( 11, 111 ),
            ( 11, 112 ),
        ) )
        
        self.assertStatsValue( 'ArcCount', 4 )
        
        arcs = self.gp.capture_list_successors( 1 )
        
        self.assertTrue( TestBase.setEquals( arcs, (
            ( 11, ),
            ( 12, ),
        ) ), "sucessors of (1)" )
        
        arcs = self.gp.capture_list_successors( 11 )
        self.assertTrue( TestBase.setEquals( arcs, (
            ( 111, ),
            ( 112, ),
        ) ), "sucessors of (2)" )

        # ------------------------------------------------------
        
        self.gp.add_arcs( (
            ( 1, 11 ),
            ( 11, 112 ),
            ( 2, 21 ),
        ) )

        self.assertStatsValue( 'ArcCount', 5 )

        arcs = self.gp.capture_list_successors( 2 )
        self.assertTrue( TestBase.setEquals( arcs, (
            ( 21, ),
        ) ), "sucessors of (2)" )
        
    
    
    def test_clear(self):
        self.gp.add_arcs( (
            ( 1, 11 ),
            ( 1, 12 ),
            ( 11, 111 ),
            ( 11, 112 ),
        ) )
        
        self.assertStatsValue( 'ArcCount', 4 )
        
        self.gp.clear()
        
        arcs = self.gp.capture_list_successors( 1 )
        
        self.assertEmpty( arcs )
        self.assertStatsValue( 'ArcCount', 0 )

        #--------------------------------------------
        self.gp.add_arcs( (
            ( 1, 11 ),
            ( 1, 12 ),
            ( 11, 111 ),
            ( 11, 112 ),
        ) )
        
        self.assertStatsValue( 'ArcCount', 4 )
    

    def test_traverseSuccessors(self):
        self.gp.add_arcs( (
            ( 1, 11 ),
            ( 1, 12 ),
            ( 11, 111 ),
            ( 11, 112 ),
            ( 111, 1111 ),
            ( 111, 1112 ),
            ( 112, 1121 ),
        ) )
        
        self.assertStatsValue( 'ArcCount', 7 )
        
        #--------------------------------------------
        succ = self.gp.capture_traverse_successors( 11, 5 )

        self.assertEquals( [ (11,), (111,), (112,), (1111,), (1112,), (1121,), ], succ )
    
    
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
    
    def test_setMeta(self):
        #define var
        self.gp.set_meta("foo", 1234)
        val = self.gp.get_meta_value("foo")
        self.assertEquals( "1234", val )
        
        #redefine var
        self.gp.set_meta("foo", "bla/bla")
        val = self.gp.get_meta_value("foo")
        self.assertEquals( "bla/bla", val )
        
        # test bad -----------------------------------------
        try:
            self.gp.set_meta("...", 1234)
            self.fail( "exception expected" )
        except gpException as ex:
            pass

        try:
            self.gp.set_meta("x y", 1234)
            self.fail( "exception expected" )
        except gpException as ex:
            pass

        try:
            self.gp._set_meta("  ", 1234)
            self.fail( "exception expected" )
        except gpException as ex:
            pass

        try:
            self.gp.set_meta("foo", "bla bla")
            self.fail( "exception expected" )
        except gpException as ex:
            pass

        try:
            self.gp.set_meta("foo", "2<3")
            self.fail( "exception expected" )
        except gpException as ex:
            pass

    def test_getMeta(self):
        #get undefined
        val = self.gp.try_get_meta_value("foo")
        self.assertEquals( False, val )
        
        #set var, and get value
        self.gp.set_meta("foo", "xxx")
        val = self.gp.get_meta_value("foo")
        self.assertEquals( "xxx", val )
        
        #remove var, then get value
        self.gp.remove_meta("foo")
        val = self.gp.try_get_meta_value("foo")
        self.assertEquals( False, val )

    def test_removeMeta(self):
        #remove undefined
        ok = self.gp.try_remove_meta("foo")
        self.assertEquals( False, ok )
        
        #set var, then remove it
        self.gp.set_meta("foo", "xxx")
        ok = self.gp.try_remove_meta("foo")
        self.assertEquals( "OK", ok )

    def test_listMeta(self):
        # assert empty
        meta = self.gp.capture_list_meta()
        self.assertEmpty( meta )
        
        # add one, assert list
        self.gp.set_meta("foo", 1234)
        meta = self.gp.capture_list_meta_map()
        self.assertEquals( { "foo": 1234}, meta )
        
        # remove one, assert empty
        self.gp.remove_meta("foo")
        meta = self.gp.capture_list_meta()
        self.assertEmpty( meta )
    
    
    #TODO: add all the tests we have in the talkback test suit
    

if __name__ == '__main__':
    unittest.main()


