from gp.mediawiki import *
from gp.client import *
from gp.mysql import *
from test_base import *

import unittest
import sys

class MediaWikiTest (SlaveTestBase, unittest.TestCase):

    def setUp(self) :
        self.dump = PipeSink( sys.stdout )

        try :
            self.gp = MediaWikiGlue.new_slave_connection( test_graphcore_path )
            self.gp.connect()
        except gpException as ex:
            print "Unable to launch graphcore instance from %s, please make sure graphcore is installed and check the test_graphcore_path configuration options in test_config.py.\nOriginal error: %s " % (test_graphcore_path, ex.getMessage() )
            suicide(10)
        

        try :
            self.gp.mysql_connect( test_mysql_host, test_mysql_user, test_mysql_password, test_mysql_database )
            self.gp.set_table_prefix( test_mediawiki_table_prefix )
        except gpException as ex:
            print "Unable to connect to database %s on MySQL host %s as %s, please make sure MySQL is running and check the test_mysql_host and related configuration options in test_cofig.py.\nOriginal error: %s " % (test_mysql_database, test_mysql_host, test_mysql_user, ex.getMessage() )
            suicide(10)
        
    

    def _makeTable( self, table, fieldSpec, temp = False ) :
        t = " TEMPORARY " if temp else ""
        sql = "CREATE " + t + " TABLE IF NOT EXISTS " + table
        sql += "("
        sql += fieldSpec
        sql += ")"
        
        self.gp.mysql_query(sql)
        
        sql = "TRUNCATE TABLE " + table
        self.gp.mysql_query(sql)
    

    def _makeWikiTable( self, name, spec ) :
        name = test_mediawiki_table_prefix + name
        
        self._makeTable( name, spec )
        return name
    

    def _makeWikiStructure( self ) :
        p = self._makeWikiTable( "page", "page_id INT NOT NULL, page_namespace INT NOT NULL, page_title VARCHAR(255) NOT NULL, PRIMARY KEY (page_id), UNIQUE KEY (page_namespace, page_title)" )
        self.gp.mysql_query( "TRUNCATE " + p )
        
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (1, " + str(NS_MAIN) + ", 'Main_Page')" )
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (2, " + str(NS_PROJECT) + ", 'Help_Out')" )
        
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (10, " + str(NS_CATEGORY) + ", 'ROOT')" )
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (20, " + str(NS_CATEGORY) + ", 'Portals')" )
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (110, " + str(NS_CATEGORY) + ", 'Topics')" )
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (1110, " + str(NS_CATEGORY) + ", 'Beer')" )
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (1111, " + str(NS_MAIN) + ", 'Lager')" )
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (1112, " + str(NS_MAIN) + ", 'Pils')" )
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (2110, " + str(NS_CATEGORY) + ", 'Cheese')" )
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (120, " + str(NS_CATEGORY) + ", 'Maintenance')" )
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (1120, " + str(NS_CATEGORY) + ", 'Bad_Cheese')" )
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (1122, " + str(NS_MAIN) + ", 'Toe_Cheese')" )
        self.gp.mysql_query( "INSERT INTO " + p + " VALUES (333, " + str(NS_TEMPLATE) + ", 'Yuck')" )
        
        cl = self._makeWikiTable( "categorylinks", "cl_from INT NOT NULL, cl_to VARCHAR(255) NOT NULL, PRIMARY KEY (cl_from, cl_to), INDEX cl_to (cl_to)" )
        self.gp.mysql_query( "TRUNCATE " + cl )
        
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (1, 'Portals')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (2, 'Portals')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (20, 'ROOT')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (120, 'ROOT')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (110, 'ROOT')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (1110, 'Topics')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (2110, 'Topics')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (1111, 'Beer')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (1112, 'Beer')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (1120, 'Maintenance')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (1120, 'Cheese')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (1120, 'Cruft')" )
        self.gp.mysql_query( "INSERT INTO " + cl + " VALUES (1122, 'Bad_Cheese')" )

        tl = self._makeWikiTable( "templatelinks", "tl_from INT NOT NULL, tl_namespace INT NOT NULL, tl_title VARCHAR(255) NOT NULL, PRIMARY KEY (tl_from, tl_namespace, tl_title), INDEX tl_to (tl_namespace, tl_title)" )
        self.gp.mysql_query( "TRUNCATE " + tl )
        
        self.gp.mysql_query( "INSERT INTO " + tl + " VALUES (1122, " + str(NS_TEMPLATE) + ", 'Yuck')" )
        self.gp.mysql_query( "INSERT INTO " + tl + " VALUES (1111, " + str(NS_TEMPLATE) + ", 'Yuck')" )
    
        
        
    ###########################################

    def test_TraverseSuccessors( self ) :
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
        succ = self.gp.capture_traverse_successors( 11, 5 )

        self.assertEquals( [ ( 11, ), ( 111, ), ( 112, ), ( 1111, ), ( 1112, ), ( 1121, ), ], succ )
    
        
    ###########################################

    def test_AddArcsFromCategoryStructure( self ) :
        self._makeWikiStructure()
        
        #-----------------------------------------------------------
        self.gp.add_arcs_from_category_structure()

        #-----------------------------------------------------------
        a = self.gp.capture_list_successors( 10 )
        self.assertEquals([( 20, ), ( 110, ), ( 120, )], a )

        a = self.gp.capture_list_predecessors( 1120 )
        self.assertEquals([( 120, ), ( 2110, )], a )

        a = self.gp.capture_traverse_successors( 110, 5 )
        self.assertEquals([( 110, ), ( 1110, ), ( 2110, ), ( 1120, )], a )
    

    def test_GetSubcategories( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        #-----------------------------------------------------------
        a = self.gp.get_subcategories("topics", 5)
        self.assertEquals([( "Topics", ), 
                                    ( "Beer", ), 
                                    ( "Bad_Cheese", ), 
                                    ( "Cheese", )], a )

        #-----------------------------------------------------------
        a = self.gp.get_subcategories("topics", 5, "maintenance")
        self.assertEquals([( "Topics", ), 
                                    ( "Beer", ), 
                                    ( "Cheese", )], a )
    

    ###########################################
    def test_AddSubcategories( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        pages = PageSet(self.gp)
        pages.create_table()
        
        #-----------------------------------------------------------
        pages.clear()
        ok = pages.add_subcategories("topics", 5)
        self.assertTrue( ok )
        
        a = pages.capture()
        self.assertEquals([(110, NS_CATEGORY, "Topics"), 
                                    (1110, NS_CATEGORY, "Beer"), 
                                    (1120, NS_CATEGORY, "Bad_Cheese"), 
                                    (2110, NS_CATEGORY, "Cheese")], a )
        
        #-----------------------------------------------------------
        pages.clear()
        ok = pages.add_subcategories("Portals", 5)
        self.assertTrue( ok )
        
        a = pages.capture()
        self.assertEquals([(20, NS_CATEGORY, "Portals")], a )

        #-----------------------------------------------------------
        pages.dispose()
    
    
    def test_AddPagesTranscluding( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        pages = PageSet(self.gp)
        pages.create_table()
        
        #-----------------------------------------------------------
        pages.clear()
        ok = pages.add_pages_transclusing("yuck")
        self.assertTrue( ok )
        
        a = pages.capture()
        self.assertEquals([(1111, NS_MAIN, "Lager"), 
                                    (1122, NS_MAIN, "Toe_Cheese")], a )
        
        #-----------------------------------------------------------
        pages.dispose()
    
    
    def test_AddPagesIn( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        pages = PageSet(self.gp)
        pages.create_table()
        
        #-----------------------------------------------------------
        pages.clear()
        ok = pages.add_pages_in("topics", None, 5)
        self.assertTrue( ok )
        
        a = pages.capture()
        expected = [ (110, NS_CATEGORY, "Topics"), 
                                    (1110, NS_CATEGORY, "Beer"), 
                                    (1111, NS_MAIN, "Lager"), 
                                    (1112, NS_MAIN, "Pils"), 
                                    (1120, NS_CATEGORY, "Bad_Cheese"), 
                                    (1122, NS_MAIN, "Toe_Cheese"), 
                                    (2110, NS_CATEGORY, "Cheese") ]
        
        self.assertEquals(expected, a )

        #-----------------------------------------------------------
        pages.clear()
        ok = pages.add_pages_in("topics", None, 5)
        self.assertTrue( ok )
        
        a = pages.capture( NS_MAIN )
        self.assertEquals([(1111, NS_MAIN, "Lager"), 
                                    (1112, NS_MAIN, "Pils"), 
                                    (1122, NS_MAIN, "Toe_Cheese")], a )

        #-----------------------------------------------------------
        pages.clear()
        ok = pages.add_pages_in("Portals", NS_MAIN, 5)
        self.assertTrue( ok )
        
        a = pages.capture()
        self.assertEquals([(1, NS_MAIN, "Main_Page"),
                                    (20, NS_CATEGORY, "Portals")], a )

        #-----------------------------------------------------------
        pages.clear()
        ok = pages.add_pages_in("portals", (NS_MAIN, NS_PROJECT), 5)
        self.assertTrue( ok )
        
        a = pages.capture( (NS_MAIN, NS_PROJECT) )
        self.assertEquals([(1, NS_MAIN, "Main_Page"), 
                                    (2, NS_PROJECT, "Help_Out")], a )

        #-----------------------------------------------------------
        pages.dispose()
    

    def test_BufferedAddPagesIn( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        pages = PageSet(self.gp)
        pages.set_expect_big(False)
        pages.create_table()
        
        #-----------------------------------------------------------
        pages.clear()
        ok = pages.add_pages_in("topics", None, 5)
        self.assertTrue( ok )
        
        a = pages.capture()
        expected = [(110, NS_CATEGORY, "Topics"), 
                                    (1110, NS_CATEGORY, "Beer"), 
                                    (1111, NS_MAIN, "Lager"), 
                                    (1112, NS_MAIN, "Pils"), 
                                    (1120, NS_CATEGORY, "Bad_Cheese"), 
                                    (1122, NS_MAIN, "Toe_Cheese"), 
                                    (2110, NS_CATEGORY, "Cheese") ]
        
        self.assertEquals(expected, a )

        #-----------------------------------------------------------
        pages.dispose()
    

    def test_SubtractPageSet( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        pages = PageSet(self.gp)
        pages.create_table()
        
        rpages = PageSet(self.gp)
        rpages.create_table()
        
        #-----------------------------------------------------------
        ok = pages.add_pages_in("topics", None, 5)
        ok = rpages.add_pages_in("Maintenance", None, 5)

        ok = pages.subtract_page_set( rpages )
        self.assertTrue( ok )
        
        a = pages.capture()
        expected = [ (110, NS_CATEGORY, "Topics"), 
                                    (1110, NS_CATEGORY, "Beer"), 
                                    (1111, NS_MAIN, "Lager"), 
                                    (1112, NS_MAIN, "Pils"), 
                                    (2110, NS_CATEGORY, "Cheese") ]
        
        self.assertEquals(expected, a )
        
        #-----------------------------------------------------------
        pages.dispose()
        rpages.dispose()
    

    def test_RetainPageSet( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        pages = PageSet(self.gp)
        pages.create_table()
        
        rpages = PageSet(self.gp)
        rpages.create_table()
        
        #-----------------------------------------------------------
        ok = pages.add_pages_in("topics", None, 5)
        ok = rpages.add_pages_in("Maintenance", None, 5)

        ok = pages.retain_page_set( rpages )
        self.assertTrue( ok )
        
        a = pages.capture()
        expected = [ (1120, NS_CATEGORY, "Bad_Cheese"), 
                            (1122, NS_MAIN, "Toe_Cheese") ]
        
        self.assertEquals(expected, a )
        
        #-----------------------------------------------------------
        pages.dispose()
        rpages.dispose()
    

    def test_AddPageSet( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        beer = PageSet(self.gp)
        beer.create_table()
        
        cheese = PageSet(self.gp)
        cheese.create_table()
        
        #-----------------------------------------------------------
        ok = cheese.add_pages_in("Cheese", None, 5)
        ok = beer.add_pages_in("Beer", None, 5)

        ok = cheese.add_page_set( beer )
        self.assertTrue( ok )
        
        a = cheese.capture()
        expected = [ (1110, NS_CATEGORY, "Beer"), 
                            (1111, NS_MAIN, "Lager"), 
                            (1112, NS_MAIN, "Pils"), 
                            (1120, NS_CATEGORY, "Bad_Cheese"), 
                            (1122, NS_MAIN, "Toe_Cheese"),
                            (2110, NS_CATEGORY, "Cheese")       ]
        
        self.assertEquals(expected, a )
        
        #-----------------------------------------------------------
        beer.dispose()
        cheese.dispose()
    

    def test_DeleteWhere( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        pages = PageSet(self.gp)
        pages.create_table()
        
        pages.add_pages_in("topics", None, 5)
        
        #-----------------------------------------------------------
        pages.delete_where( "where page_namespace = %i" % NS_CATEGORY )
        
        a = pages.capture()
        expected = [ (1111, NS_MAIN, "Lager"), 
                            (1112, NS_MAIN, "Pils"), 
                            (1122, NS_MAIN, "Toe_Cheese") ]
        
        self.assertEquals(expected, a )
        
        #-----------------------------------------------------------
        pages.dispose()
    

    def test_DeleteUsing( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        pages = PageSet(self.gp)
        pages.create_table()
        
        pages.add_pages_in("topics", None, 5)
        
        #-----------------------------------------------------------
        sql = " JOIN " + self.gp.wiki_table("templatelinks") + " as X "
        sql += " ON T.page_id = X.tl_from "
        sql += " WHERE X.tl_namespace = %i" % NS_TEMPLATE
        sql += " AND X.tl_title = " + self.gp.quote_string("Yuck")
        
        pages.delete_using( sql )
        
        a = pages.capture(NS_MAIN)
        expected = [ (1112, NS_MAIN, "Pils") ]
        
        self.assertEquals(expected, a )
        
        #-----------------------------------------------------------
        pages.dispose()
    

    def test_StripNamespace( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        pages = PageSet(self.gp)
        pages.create_table()
        
        #-----------------------------------------------------------
        pages.clear()
        pages.add_pages_in("topics", None, 5)
        pages.strip_namespace( NS_CATEGORY )
        
        a = pages.capture()
        expected = [ (1111, NS_MAIN, "Lager"), 
                            (1112, NS_MAIN, "Pils"), 
                            (1122, NS_MAIN, "Toe_Cheese") ]
        
        self.assertEquals(expected, a )
        
        #-----------------------------------------------------------
        pages.clear()
        pages.add_pages_in("Portals", None, 5)
        pages.strip_namespace( (NS_CATEGORY, NS_PROJECT) )
        
        a = pages.capture()
        expected = [ (1, NS_MAIN, "Main_Page") ]
        
        self.assertEquals(expected, a )
        
        #-----------------------------------------------------------
        pages.dispose()
    

    def test_RetainNamespace( self ) :
        self._makeWikiStructure()
        self.gp.add_arcs_from_category_structure()

        pages = PageSet(self.gp)
        pages.create_table()
        
        #-----------------------------------------------------------
        pages.clear()
        pages.add_pages_in("topics", None, 5)
        pages.retain_namespace( (NS_MAIN,) )
        
        a = pages.capture()
        expected = [ (1111, NS_MAIN, "Lager"), 
                            (1112, NS_MAIN, "Pils"), 
                            (1122, NS_MAIN, "Toe_Cheese") ]
        
        self.assertEquals(expected, a )
        
        #-----------------------------------------------------------
        pages.clear()
        pages.add_pages_in("Portals", None, 5)
        pages.retain_namespace( NS_MAIN )
        
        a = pages.capture()
        expected = [ (1, NS_MAIN, "Main_Page") ]
        
        self.assertEquals(expected, a )
    

    
if __name__ == '__main__':
    unittest.main()


