from client import *
from mysql import *

import re

NS_MAIN = 0
NS_TALK = 1
NS_USER = 2
NS_USER_TALK = 3
NS_PROJECT = 4
NS_PROJECT_TALK = 5
NS_FILE = 6
NS_FILE_TALK = 7
NS_MEDIAWIKI = 8
NS_MEDIAWIKI_TALK = 9
NS_TEMPLATE = 10
NS_TEMPLATE_TALK = 11
NS_HELP = 12
NS_HELP_TALK = 13
NS_CATEGORY = 14
NS_CATEGORY_TALK = 15


class MediaWikiGlue (MySQLGlue) :
    
    def __init__( self, transport, graphname = None ) :
        super(MediaWikiGlue, self).__init__(transport, graphname)
        
        self.table_prefix = ""
        
        #h = array( self, 'gp_mediawiki_exec_handler' )
        #self.addExecHandler( h )
    
    
    def set_table_prefix ( self, prefix ) :
        self.table_prefix = prefix
    
    
    def get_db_key ( self, name ) :
        if name is None or name == False:
            raise gpUsageException("name must not be empty!")
        
        #TODO: use native MediaWiki method if available
        name = name.strip()

        if name == "":
            raise gpUsageException("name must not be empty!")

        name = re.sub(' ', '_', name)
        
        result = name[0].upper() + name[1:] #FIXME: unreliable, handle unicode!
        
        return name
    

    def wiki_table ( self, name ) :
        return self.table_prefix + name
    
    
    def get_page_id ( self, ns, title ) :
        sql = "select page_id from " + self.wiki_table( "page" )
        sql += " where page_namespace = %i" % int(ns)
        sql += " and page_title = " + self.quote_string( self.get_db_key(title) )
        
        id = self.mysql_query_value( sql )
        return id
    
    
    def add_arcs_from_category_structure ( self, ) :
        sql = "select C.page_id as parent, P.page_id as child"
        sql += " from " + self.wiki_table( "page" ) + " as P "
        sql += " join " + self.wiki_table( "categorylinks" ) + " as X "
        sql += " on X.cl_from = P.page_id "
        sql += " join " + self.wiki_table( "page" ) + " as C "
        sql += " on C.page_namespace = %i" % NS_CATEGORY
        sql += " and C.page_title = X.cl_to "
        sql += " where P.page_namespace = %i" % NS_CATEGORY
        
        src = self.make_source( MySQLSelect( sql ) )
        
        self.add_arcs( src )
        src.close()
    
     
    def get_subcategories ( self, cat, depth, without = None, without_depth = None ) :
        sink = ArraySink()
        
        id = self.get_page_id( NS_CATEGORY, cat )
        if ( not id ): return 'NONE'
        
        if ( without ): without_id = self.get_page_id( NS_CATEGORY, without )
        else: without_id = False

        temp = self.make_temp_sink( MySQLTable('?', 'id') )
        
        if ( without_id ) :
            if ( not without_depth ): without_depth = depth
            status = self.traverse_successors_without( id, depth, without_id, without_depth, temp )
        else :
            status = self.traverse_successors( id, depth, temp )
        
        
        temp.close()
        
        if ( status == 'OK' ) :
            sql = "select page_title "
            sql += " from " + self.wiki_table( "page" )
            sql += " join " + temp.getTable().get_name()
            sql += " on id = page_id "
            sql += " where page_namespace = %i" % NS_CATEGORY # should be redundant
            sql += " order by page_id "
            
            self.select_into( sql , sink)
        
        
        temp.drop()
        
        return sink.getData()
     
    @staticmethod
    def new_client_connection( graphname, host = False, port = False ) :
        return MediaWikiGlue( ClientTransport(host, port), graphname ) #FIXME: PORT graphname stuff to PHP!
    
    @staticmethod
    def new_slave_connection( command, cwd = None, env = None ) :
        return MediaWikiGlue( SlaveTransport(command, cwd, env), None )
    



class PageSet :
    
    def __init__ ( self, glue, table = "?", id_field = "page_id", namespace_field = "page_namespace", title_field = "page_title", big = True ) :
        self.big = big

        self.glue = glue
        self.table = table
        
        self.id_field = id_field
        self.namespace_field = namespace_field
        self.title_field = title_field
        
        self.table_obj = MySQLTable( self.table, self.id_field, self.namespace_field, self.title_field )
        self.table_obj.set_field_definition( self.id_field, "INT NOT NULL")
        self.table_obj.set_field_definition( self.namespace_field, "INT DEFAULT NULL")
        self.table_obj.set_field_definition( self.title_field, "VARCHAR(255) BINARY DEFAULT NULL")
        self.table_obj.add_key_definition( "PRIMARY KEY (" + self.id_field + ")" )
        self.table_obj.add_key_definition( "UNIQUE KEY (" + self.namespace_field + ", " + self.title_field + ")" )
        
        self.table_id_obj = MySQLTable( self.table, self.id_field )
        self.table_id_obj.add_key_definition( "PRIMARY KEY (" + self.id_field + ")" )
    
    
    def set_expect_big ( self, big ) :
        self.big = big
    
    
    def get_table ( self, ) :
        return self.table_obj
    
    
    def create_table ( self, ) :
        table = self.table
        t = ""
        
        if ( not table  or  table == '?' ) :
            table = "gp_temp_%s" % self.glue.next_id()
            t = " TEMPORARY "
        
        
        sql = "CREATE " + t + " TABLE " + table
        sql += "("
        sql += self.table_obj.get_field_definitions()
        sql += ")"
        
        self._update(sql)
        
        self.table = table
        self.table_obj.set_name( self.table )
        self.table_id_obj.set_name( self.table )

        return table
        
    
    
    def _query( self, sql, **kwargs ) :
        if not 'unbuffered' in kwargs:
            kwargs['unbuffered'] = self.big
        
        return self.glue.mysql_query(sql, **kwargs) #TODO: port kwargs to PHP
    
    def _update( self, sql, **kwargs ) : #TODO: port to PHP; use in PHP!
        return self.glue.mysql_update(sql, **kwargs)
    
    def add_from_select ( self, select, comment = None ) :
        sql= "REPLACE INTO " + self.table + " "
        sql += "( "
        sql += self.id_field + ", "
        sql += self.namespace_field + ", "
        sql += self.title_field + " ) "
        sql += select
        
        return self._update( sql, comment = comment )
    
    
    def delete_where ( self, where, comment = None ) :
        sql= "DELETE FROM " + self.table + " "
        sql += where
        
        return self._update( sql, comment = comment )
    
    
    def delete_using ( self, using, tableAlias = "T", comment = None ) :
        sql= "DELETE FROM " + tableAlias + " "
        sql += "USING " + self.table + " AS " + tableAlias + " "
        sql += using
        
        return self._update( sql, comment = comment )
    
    
    def resolve_ids ( self, comment = None ) :
        #NOTE: MySQL can't perform self-joins on temp tables. so we need to copy the ids to another temp table first.
        t = MySQLTable("?", "page_id")
        t.add_key_definition("PRIMARY KEY (page_id)")
        
        tmp = self.glue.make_temp_table( t )
        
        sql = tmp.get_insert(True)
        sql += "SELECT " + self.id_field
        sql += " FROM " +  self.table
        sql += " WHERE page_title IS NULL"
        
        self._update( sql );  #copy page ids with no page title into temp table
        
        sql = "SELECT P.page_id, P.page_namespace, P.page_title "
        sql += " FROM " + self.glue.wiki_table("page") + " AS P "
        sql += " JOIN " + tmp.get_name() + " AS T ON T.page_id = P.page_id"
        
        self.add_from_select( sql, comment = comment ) #TODO: port comment to PHP
        
        self.glue.drop_temp_table( tmp )
        return True
    

    def make_sink ( self, ) :
        sink = self.glue.make_sink( self.table_obj )
        return sink
    

    def make_id_sink ( self, ) :
        sink = self.glue.make_sink( self.table_id_obj )
        return sink
    

    def make_id_source ( self, ns = None ) :
        return self.make_source( ns, True )
    

    def make_source ( self, ns = None, ids_only = False, auto_order = False ) : #TODO: PORT auto_order to PHP
        t = self.table_id_obj if ids_only else self.table_obj
        
        if ( ns is not None ) :
            select = t._get_select()
            
            if ( isinstance(ns, (tuple, list, set)) ): select += " where page_namespace in " + self.glue.as_list( ns )
            else: select += " where page_namespace = %i" % int(ns)
            
            t = MySQLSelect(select)
        
        
        src = self.glue.make_source( t, big = self.big, auto_order = auto_order )
        return src
    

    def capture ( self, ns = None, data = None ) :
        sink = ArraySink( data )
        self.copy_to_sink( ns, sink )
        return sink.getData()
    

    def capture_ids ( self, ns = None, data = None ) :
        sink = ArraySink( data )
        self.copy_ids_to_sink( ns, sink )
        return sink.getData()
    

    def copy_to_sink ( self, ns, sink ) :
        src = self.make_source(ns)
        c = self.glue.copy(src, sink, "~")
        src.close()
        return c
    

    def copy_ids_to_sink ( self, ns, sink ) :
        src = self.make_id_source(ns)
        c = self.glue.copy(src, sink, "~")
        src.close()
        return c
    

    def add_source ( self, src ) :
        sink = self.make_sink()
        c = self.glue.copy( src, sink, "+" )
        sink.close()
        return c
    

    def add_page_set ( self, set ) :
        select = set.get_table()._get_select()
        return self.add_from_select( select )
    

    def subtract_page_set ( self, set ) :
        t = set.get_table()
        return self.subtract_table( t )
    

    def subtract_source ( self, src ): #XXX: must be a 1 column id source...
        t = MySQLTable("?", "page_id")
        sink = self.glue.make_temp_sink( t )
        t = sink.getTable()
        
        self.glue.copy( src, sink, "+" )
        
        ok = self.subtract_table(t, "page_id")
        
        self.glue.drop_temp_table(t)
        sink.close()
        
        return ok
    

    def retain_page_set ( self, set ) :
        t = set.get_table()
        return self.retain_table( t )
    

    def retain_source ( self, src ) : #XXX: must be a 1 column id source...
        t = MySQLTable("?", "page_id")
        sink = self.glue.make_temp_sink( t )
        t = sink.getTable()
        
        self.glue.copy( src, sink, "+" )
        
        ok = self.retain_table(t, "page_id")
        
        self.glue.drop_temp_table(t)
        sink.close()
        
        return ok
    

    def subtract_table ( self, table, id_field = None ) :
        if ( not id_field ): id_field = table.get_field1()
        
        sql = "DELETE FROM T "
        sql += " USING " + self.table + " AS T "
        sql += " JOIN " + table.get_name() + " AS R "
        sql += " ON T." + self.id_field + " = R." + id_field
        
        self._update(sql)
        return True
    

    def retain_table ( self, table, id_field = None ) :
        if ( not id_field ): id_field = table.get_field1()
        
        sql = "DELETE FROM T "
        sql += " USING " + self.table + " AS T "
        sql += " LEFT JOIN " + table.get_name() + " AS R "
        sql += " ON T." + self.id_field + " = R." + id_field
        sql += " WHERE R." + id_field + " IS NULL"
        
        self._update(sql)
        return True
    

    def remove_page ( self, ns, title ) :
        sql = "DELETE FROM " + self.table
        sql += " WHERE " + self.namespace_field + " = %i" % int(ns)
        sql += " AND " + self.title_field + " = " + self.glue.quote_string(title)
        
        self._update(sql)
        return True
    
    
    def remove_page_id ( self, id ) :
        sql = "DELETE FROM " + self.table
        sql += " WHERE " + self.id_field + " = %i" % int(id)
        
        self._update(sql)
        return True
    

    def strip_namespace ( self, ns, inverse = False ) :
        sql = "DELETE FROM " + self.table
        sql += " WHERE " + self.namespace_field
        
        if ( isinstance(ns, (tuple, list, set)) ): sql +=  ( " not in " if inverse else " in " ) + self.glue.as_list( ns )
        else: sql += ( " != " if inverse else " = " ) + str(int(ns))
            
        self._update(sql)
        return True
    

    def retain_namespace ( self, ns ) :
        return self.strip_namespace( ns, True )
    
    
    def add_page ( self, id, ns, title ) :
        if ( not id ): id = self.glue.get_page_id( NS_CATEGORY, cat )
        
        values = array(id, ns, title)
        
        sql = self.table_obj.insert_command()
        sql += " VALUES "
        sql += self.glue.as_list(values)
        
        self._update( sql )
        return True
    

    def add_page_id ( self, id ) :
        values = array(id)
        
        sql = "INSERT IGNORE INTO " + self.table
        sql += " ( " + self.id_field + " ) "
        sql += " VALUES "
        sql += self.glue.as_list(values)
        
        self._update( sql )
        return True
    
    
    def expand_categories ( self, ns = None, comment = None ) :
        #NOTE: MySQL can't perform self-joins on temp tables. so we need to copy the category names to another temp table first.
        t = MySQLTable("?", "cat_title")
        t.set_field_definition("cat_title", "VARCHAR(255) BINARY NOT NULL")
        t.add_key_definition("PRIMARY KEY (cat_title)")
        
        tmp = self.glue.make_temp_table( t )
        
        sql = tmp.get_insert(True)
        sql += " select page_title "
        sql += " from " + self.table + " as T "
        sql += " where page_namespace = %i " % NS_CATEGORY
    
        self._update( sql )
        #self.glue.dump_query("select * from " +tmp.get_name())
        
        # ----------------------------------------------------------
        sql = "select P.page_id, P.page_namespace, P.page_title "
        sql += " from " + self.glue.wiki_table( "page" ) + " as P "
        sql += " join " + self.glue.wiki_table( "categorylinks" ) + " as X "
        sql += " on X.cl_from = P.page_id "
        sql += " join " + tmp.get_name() + " as T "
        sql += " on T.cat_title = X.cl_to "
        
        if (ns is not None) :
            if ( isinstance(ns, (tuple, list, set)) ): sql += " where P.page_namespace in " + self.glue.as_list( ns )
            else: sql += " where P.page_namespace = %i" % int(ns)
        
    
        #self.glue.dump_query(sql)
        self.add_from_select( sql, comment = comment ) #TODO: port comment to PHP
        
        #self.glue.dump_query("select * from " +self.table)
        self.glue.drop_temp_table( tmp )
        return True
    
    
    def add_subcategories ( self, cat, depth, without = None, without_depth = None ) :
        self._add_subcategory_ids(cat, depth, without, without_depth)
        self.resolve_ids()
        return True
    
    
    def _add_subcategory_ids( self, cat, depth, without = None, without_depth = None ) :
        id = self.glue.get_page_id( NS_CATEGORY, cat )
        if ( not id ): return False
        
        if ( without ): without_id = self.glue.get_page_id( NS_CATEGORY, without )
        else: without_id = False

        sink = self.make_id_sink()
        
        if ( without_id ) :
            if ( not without_depth ): without_depth = depth
            status = self.glue.traverse_successors_without( id, depth, without_id, without_depth, sink )
        else :
            status = self.glue.traverse_successors( id, depth, sink )
        
        
        sink.close()
        return True
    
    def get_size(self):
        res = self._query("SELECT COUNT(*) FROM " + self.table)
        try:
            row = res.fetchone()
        finally:
            res.close()
        
        return row[0]
    
    def add_pages_in ( self, cat, ns, depth, comment = None ) :
        self.get_size()
        
        if ( not self.add_subcategories(cat, depth) ): 
            return False

        self.get_size() # ?!

        self.expand_categories(ns, comment = comment)
        return True
    

    def add_pages_transclusing ( self, tag, ns = None, comment = None ) :
        if ( ns is None ): ns = NS_TEMPLATE
        tag = self.glue.get_db_key( tag )

        sql = " SELECT page_id, page_namespace, page_title "
        sql += " FROM " + self.glue.wiki_table( "page" )
        sql += " JOIN " + self.glue.wiki_table( "templatelinks" )
        sql += " ON tl_from = page_id "
        sql += " WHERE tl_namespace = %i" % int(ns)
        sql += " AND tl_title = " + self.glue.quote_string(tag)
        
        return self.add_from_select(sql, comment = comment)
    

    def clear ( self, ) :
        sql = "TRUNCATE " + self.table
        self._update(sql)
        return True
    

    def dispose ( self, ) :
        sql = "DROP TEMPORARY TABLE " + self.table
        self._update(sql)
        return True
    


