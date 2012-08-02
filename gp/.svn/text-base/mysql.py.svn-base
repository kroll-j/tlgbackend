from client import *
from client import __function__

import types
import re
import MySQLdb, MySQLdb.cursors
import warnings

class MySQLSource (DataSource):
    
    def __init__(self, result, table):
        self.result = result
        self.table = table
    

    def next(self):
        # XXX: if we knew that the order of fields in the result set is the same
        #     as the order given in self.table, we could just use result.fetchone()
        
        raw = _fetch_dict( self.result )
        
        if not raw: 
            raise StopIteration()
        
        row = ( raw.get( f ) for f in self.table.get_fields() )
                
        return tuple( row )
    
    
    def close (self):
        self.result.close()
    

def strip_qualifier(self, n ):
    return re.sub(r'^.*\.', '', n)

class MySQLTable (object):

    def __init__(self, name, *fields):
        self.name = name
        
        self.field_definitions = {}
        self.key_definitions = []
        
        if ( isinstance(fields[0], (tuple, list) ) ): self.fields = fields[0]
        else: self.fields = fields
        
        for f in self.fields:
            if ( not f ): raise gpUsageException( "empty field name!" )
        
        
        #for ( i = count(self.fields) -1; i >= 0; i-- ):
            #if ( self.fields[i] ) break
        
        
        #if  i+1 < count(self.fields) :
            #self.fields = array_slice(self.fields, 0, i+1)
    
    
    def set_name( self, name ):
        self.name = name
    
    
    def set_fields(self, field ):
        self.fields = fields
    
    
    def set_field_definition(self, field, decl ):
        self.field_definitions[field] = decl
    
    
    def add_key_definition(self, keyDef ):
        self.key_definitions.append( keyDef )
    
    
    def get_name(self,):
        return self.name
      

    
    def get_field(self, n ):
        return self.fields[ n-1 ]
    
    
    def get_field1(self, basename_only = False ):
        if ( basename_only ): return strip_qualifier( self.get_field(1) )
        else: return self.get_field(1)
    
    
    def get_field2(self, basename_only = False ):
        if ( basename_only ): return strip_qualifier( self.get_field(2) )
        else: return self.get_field(2)
    

    def get_fields(self,):
        return self.fields
      

    def get_field_list(self,):
        return ", ".join( self.fields )
      
    
    def get_field_definitions(self,):
        s = ""
        
        for f in self.fields:
            if ( not f ): continue #XXX: should not happen!
            if ( len(s) > 0 ) : s+= ", "
            
            if ( f in self.field_definitions ) : 
                s += " %s %s " % (f, self.field_definitions[f])
            else: 
                s += f + " INT NOT NULL "
        
        
        for k in self.key_definitions:
            if ( len(s)>0 ): s+= ", "
            s += k
        

        return s
    

    def _get_select(self,):
        return "SELECT " + self.get_field_list() + " FROM " + self.get_name()
      

    def get_insert(self, ignore = False ):
        ig = "IGNORE" if ignore else ""
        return "INSERT " + ig + " INTO " + self.get_name() + " ( " + self.get_field_list() + " ) "
      

    def get_order_by(self,):
        return "ORDER BY %s" % self.get_field_list()
      
    


class MySQLSelect (MySQLTable):
   
    def __init__(self, select):
        m = re.search(r'^\s*select\s+(.*?)\s+from\s+([^ ]+)(?:\s+(.*))?', select, flags = re.IGNORECASE + re.DOTALL)
        
        if m:
            self.select = select
            
            n = m.group(2)
            ff = re.split(r'\s*,\s*', m.group(1) )
            
            for i in range(len(ff)):
                f = ff[i]
                f = re.sub(r'^.*\s+AS\s+', '', f, flags = re.IGNORECASE) # use alias if defined
                ff[i] = f
            
            super(MySQLSelect,self).__init__(n, ff)
        else:
            raise gpUsageException("can't parse statement: %s" % select)
        
    

    def _get_select(self,):
        return self.select
      

    def get_insert(self, ignore = False ):
        raise gpUsageEsxception("can't create insert statement for: %s" % self.select)
      


class MySQLInserter (object):
    def __init__ ( self, glue, table ):
        self.glue = glue
        self.table = table
        self.fields = None
    
    def insert(self, values ):
        raise NotImplementedError( "`insert()' not implemented by %s" % self.__class__ )

    def flush (self):
        pass
    
    def close (self):
        self.flush()
    


class MySQLSimpleInserter (MySQLInserter):

    def as_list (self, values ):
        return self.glue.as_list( values )
    
    
    def _insert_command(self):
        return self.table.get_insert()
    
    
    def insert (self, values ):
        sql = self._insert_command()
        sql += " VALUES "
        sql += self.as_list(values)
        
        self.glue.mysql_update( sql )
    



class MySQLBufferedInserter (MySQLSimpleInserter):

    def __init__(self, glue, table ):
        super(MySQLBufferedInserter,self).__init__(glue, table)
        self.buffer = ""
    

    def insert (self, values ):
        vlist = self.as_list(values)
        max = self.glue.get_max_allowed_packet()

        if len(self.buffer)>0 and ( len(self.buffer) + len(vlist) + 2 ) >= max  :
            self.flush()
        
        
        if len(self.buffer) == 0:
            self.buffer = self._insert_command()
            self.buffer += " VALUES "
        else:
            self.buffer += ", "
        
        self.buffer += vlist

        if len(self.buffer) >= max :
            self.flush()
        
    
    
    def flush (self):
        if len(self.buffer)>0:
            self.glue.mysql_update( self.buffer )
            self.buffer = ""


class MySQLSink (DataSink):
    
    def __init__(self, inserter ):
        self.inserter = inserter
    
    
    def putRow (self, row ):
        self.inserter.insert( row )
    
    
    def flush (self):
        self.inserter.flush()
    
    
    def close (self):
        super(MySQLSink, self).close()
        self.inserter.close()
    
    
    def drop (self):
        raise gpUsageException("only temporary sinks can be dropped")
    


class MySQLTempSink (MySQLSink):
    def __init__( self, inserter, glue, table ):
        super(MySQLTempSink, self).__init__(inserter)
        
        self.glue = glue
        self.table = table
    
    
    def drop (self):
        sql = "DROP TEMPORARY TABLE IF EXISTS %s" % self.table.get_name()
        
        ok = self.glue.mysql_update( sql )
        return ok
    
    
    def getTable (self):
        return self.table
    

    def getTableName (self):
        return self.table
        
def _fetch_dict( cursor ):
    try:
        row = cursor.fetch_dict(  )
        return row
    except AttributeError:
        pass
        
    r = cursor.fetchone()
    if r is None: return None
    
    if hasattr(r, "has_key"):
        return r # it's a dict!

    row = {}
    
    for i in range(len(cursor.description)):
        d = cursor.description[ i ]
        row[ d[0] ] = r[ i ]
    
    return row

class MySQLGlue (Connection):
    
    def __init__(self, transport, graphname = None ):
        super(MySQLGlue, self).__init__(transport, graphname)
        
        self.connection = None
        
        self.unbuffered = False
        self._update_cursor = None
        
        self.temp_table_prefix = "gp_temp_"
        self.temp_table_db = None
        
        self.addCallHandler( self.gp_mysql_call_handler )
        
        self.max_allowed_packet = None
    
    def set_unbuffered(self, unbuffered ):
        self.unbuffered = unbuffered
    
    
    def mysql_connect( self, server, username, password, db, port = 3306 ): 
        #FIXME: connection charset, etc!
        
        #try:
        self.connection = MySQLdb.connect(host=server, user=username, passwd=password, db = db, port = port) 
        
        #XXX: would be nice to wrap the exception and provide additional info. 
        #    but without exception chaining, we lose the traceback. wich is bad.
        #except MySQLdb.Error, e:
        #   try:
        #       raise gpClientException( "Failed to connect! MySQL Error %s: %s" % (e.args[0], e.args[1]) )
        #   except IndexError:
        #       raise gpClientException( "Failed to connect! MySQL Error: %s" % e )
        
        if not self.connection :
            raise gpClientException( "Failed to connect! (unknown error)" )

        # autocommit is the default. It's even needed when reading, if we want to
        # see changes during a persistent connection.
        self.mysql_autocommit(True)
        
        return True
    
    def mysql_unbuffered_query( self, sql, **kwargs ): #TODO: port kwargs to PHP
        return self.mysql_query( sql, unbuffered = True, **kwargs )
        
    def mysql_update( self, sql, **kwargs ): #TODO: port to PHP; use in PHP!
        if 'cursor' not in kwargs or not kwargs['cursor']:
            if not self.update_cursor:
                self._update_cursor = MySQLdb.cursors.SSCursor(self.connection)
                
            kwargs['cursor'] = self._update_cursor
        
        self.mysql_query( sql, unbuffered = True, dict_rows = False, **kwargs )
        
        return self.connection.affected_rows()
        
    def inject_query_markers( self, sql, *markers ): #TODO: port markers to PHP
        if markers:
            for m in markers:
                if not m: #handle explicit None, etc
                    continue
                    
                sql = re.sub( '^\s*(select|update|replace|insert|delete)\s+', '\\1 /* '+m+' */ ', sql, flags = re.IGNORECASE | re.DOTALL )
                
        return sql
        
    def mysql_query( self, sql, unbuffered = None, dict_rows = False, cursor = None, comment = None ): #TODO: port markers to PHP
        if unbuffered is None:
            unbuffered = self.unbuffered
            
        sql = self.inject_query_markers(sql, comment)
            
        if cursor:
            using_new_cursor = False
        else:
            using_new_cursor = True
                
            if unbuffered:
                if dict_rows:
                    # no buffering, returns dicts
                    cursor = MySQLdb.cursors.SSDictCursor(self.connection) # TESTME
                else:
                    # no buffering, returns tuples
                    cursor = MySQLdb.cursors.SSCursor(self.connection) # TESTME
            else:
                if dict_rows:
                    # buffers result, returns dicts
                    cursor = MySQLdb.cursors.DictCursor(self.connection) # TESTME
                else:
                    # default: buffered tuples
                    cursor = MySQLdb.cursors.Cursor(self.connection) 
        
        with warnings.catch_warnings():
            #ignore MySQL warnings. use cursor.nfo() to get them.
            warnings.simplefilter("ignore")
        
            try:
                cursor.execute( sql ) 
            except:
                if using_new_cursor:
                    cursor.close() #NOTE: *always* close the cursor if an exception ocurred.
                    raise
        
        if not dict_rows:
            # HACK: glue a fetch_dict method to a cursor that natively returns sequences from fetchone()
            # FIXME: if we do this, we for some reason retain a reference to the cursor forever!
            #        
            #m = types.MethodType(_fetch_dict, cursor, cursor.__class__)
            #setattr(cursor, "fetch_dict", m)
            pass
        else:
            # make fetch_dict an alias for fetchone
            cursor.fetch_dict = cursor.fetchone # TESTME
        
        return cursor

        #XXX: would be nice to wrap the exception and provide additional info. 
        #    but without exception chaining, we lose the traceback. wich is bad.
        #except MySQLdb.Error as e:
            #q = sql.replace('/\s+/', ' ')
            #if ( len(q) > 255 ): q = q[:252] + '...'
            
            #try:
            #   raise gpClientException( "Query failed! MySQL Error %s: %s\nQuery was: %s" % (e.args[0], e.args[1], q) )
            #except IndexError:
            #   raise gpClientException( "Query failed! MySQL Error: %s\nQuery was: %s" % (e, q) )
        

    def set_mysql_connection(self, connection ):
        self.connection = connection
    
    
    def gp_mysql_call_handler( self, gp, params ):
        # params: cmd, args, source, sink, capture, result

        cmd = params['command']
        args = params['arguments']
        source = params['source']
        sink = params['sink']
        capture = params['capture']
        result = params['result']
            
        m = re.search( r'-(from|into)$', cmd )
            
        if m:
            cmd = re.sub(r'-(from|into)?$', '', cmd)
            action = m.group(1)
            
            c = len(args)
            if not c :
                raise gpUsageException("expected last argument to be a table spec; args: %s" % (args, ))
            
            
            t = args[c-1]
            args = args[0:c-1]
            
            if isinstance(t, (str, unicode)) :
                if ( re.search( r'^.*select\s+', t, flags = re.IGNORECASE) ): 
                    t = MySQLSelect(t)
                else: 
                    t = re.split( r'\s+|\s*,\s*', t )
            
            
            if ( isinstance(t, (list, tuple)) ): t = MySQLTable( t[0], t[1:] )
            if ( not isinstance(t, MySQLTable) ): raise gpUsageException("expected last argument to be a table spec; found %s" % get_class(t))
            
            if action == 'into' :
                if ( not t.get_name()  or  t.get_name() == "?" ): sink = self.make_temp_sink( t )
                else: sink = self.make_sink( t )
                
                result = sink #XXX: quite useless, but consistent with -from
            else:
                source = self.make_source( t )
                
                result = source #XXX: a bit confusing, and only useful for temp sinks
            
        params['command'] = cmd
        params['arguments'] = args
        params['source'] = source
        params['sink'] = sink
        params['capture'] = capture
        params['result'] = result
            
        return True
    
    
    def __make_mysql_closure( self, name ):
        rc = False
        
        def call_mysql( *args ):
            if not self.connection:
                raise gpUsageException( "not connected to mysql, can't run mysql function %s" % (name,) )
                
            if not hasattr(self.connection, name):
                raise gpUsageException( "unknown mysql function: %s, not in %s" % (name, self.connection.__class__.__name__) )
                
            f = getattr(self.connection, name)
            
            #try:
            res = f( *args ) # note: f is bound to self.connection
            return res
            
            #XXX: would be nice to wrap the exception and provide additional info. 
            #    but without exception chaining, we lose the traceback. wich is bad.
            #except MySQLdb.Error, e:
                #try:
                #   raise gpClientException( "MySQL %s failed! Error %s: %s" % (name, e.args[0], e.args[1]) )
                #except IndexError:
                #   raise gpClientException( "MySQL %s failed! Error: %s" % (name, e) )
            
        return call_mysql
    
    def __getattr__( self, name ): 
        if name.startswith('mysql_'):
            f = self.__make_mysql_closure(name[6:])
            
            setattr(self, name, f) #re-use closure!
            
            return f
        else:
            return super(MySQLGlue, self).__getattr__(name)
    
    def quote_string (self, s ): #TODO: charset
        if type(s) not in (str, unicode):
            s = "%s" % s
            
        return "'" + self.connection.escape_string( s ) + "'"
    
    def as_list (self, values ):
        sql = "("

        first = True
        for v in values:
            if ( not first ): sql += ","
            else: first = False
            
            t = type(v)
            if ( v is None ): sql+= "None"
            elif ( t == int ): sql+= "%i" % v
            elif ( t == float ): sql+= "%d" % v
            elif ( t == str or t == unicode ): sql+= self.quote_string(v) #TODO: charset...
            else: raise gpUsageException("bad value type: %s" % gettype(v))
        
        
        sql += ")"
        
        return sql
    
    id = 1
    
    def next_id (self):
        MySQLGlue.id += 1
        return MySQLGlue.id
    
    def drop_temp_table (self, spec ):
        sql = "DROP TEMPORARY TABLE %s" % spec.get_name()
        self.mysql_update(sql)
    
    
    def make_temp_table (self, spec ):
        table = spec.get_name()
        
        if ( not table  or  table == '?' ):
            table = "%s%d" % (self.temp_table_prefix, self.next_id())
            
            if self.temp_table_db: 
                table = "%s.%s" % (self.temp_table_db, table);
        
        sql = "CREATE TEMPORARY TABLE %s" % table
        sql += "("
        sql += spec.get_field_definitions()
        sql += ")"
        
        self.mysql_update(sql)
        
        return MySQLTable(table, spec.get_fields())
    
    def mysql_select_db ( self, db ):
        #NOTE: the native select_db "sometimes" triggers an InterfaceError. 
        #      This is a strange issue with MySQLdb
        
        sql = "USE %s" % re.sub('[^\w]', '', db) #TODO: apply real identifier quoting!
        
        self.mysql_update( sql )

    def mysql_query_value (self, sql, **kwargs ):
        r = self.mysql_query_record( sql, **kwargs ) #TODO: port kwargs to PHP
        
        if not r: return None
        else: return r[0]
    
    def mysql_query_record (self, sql, **kwargs ):
        cursor = self.mysql_query( sql, unbuffered = True, dict_rows = False, **kwargs ) #TODO: port kwargs to PHP
        
        try:
            a = cursor.fetchone()
        finally:
            cursor.close()
        
        if ( not a ): return None
        else: return a
    
    def set_max_allowed_packet (self, size ):
        self.max_allowed_packet = size
    
    def get_max_allowed_packet (self):
        if self.max_allowed_packet is None:
            self.max_allowed_packet = self.mysql_query_value("select @@max_allowed_packet")

        if self.max_allowed_packet is None:
            self.max_allowed_packet = 16 * 1024 * 1024 #fall back to MySQL's default of 16MB
        
        return self.max_allowed_packet
    

    def select_into (self, query, sink, **kwargs ): #TODO: port kwargs to PHP
        if isinstance(query, (str, unicode)) :
            table = MySQLSelect( query )
            sql = query
        else:
            table = query
            sql = src._get_select()
        
        
        res = self.mysql_query( sql, **kwargs )
        src = MySQLSource( res, table )
        
        c = self.copy( src, sink, '+' )
        src.close()
        
        return c
    
    
    def _new_inserter(self, table ):
        return MySQLBufferedInserter( self, table )
    
    
    def make_temp_sink (self, table ):
        table = self.make_temp_table(table)
        
        ins = self._new_inserter(table)
        sink = MySQLTempSink( ins, self, table )
        
        return sink
    

    def make_sink (self, table ):
        inserter = self._new_inserter(table)
        sink = MySQLSink( inserter )
        
        return sink
    

    def make_source (self, table, big = False, auto_order = False, **kwargs ): #TODO: PORT auto_order to PHP
        sql = table._get_select()
        
        if auto_order and not re.search(r'\s+ORDER\s+BY\s+', sql, flags = re.IGNORECASE | re.DOTALL ) : #TODO: PORT auto_order to PHP
            sql += ' ' + table.get_order_by()
        
        if not 'unbuffered' in kwargs:
            kwargs['unbuffered'] = big
        
        res = self.mysql_query(sql, **kwargs) #TODO: port kwargs to PHP
        
        src = MySQLSource( res, table )
        return src
    

    def query_to_file (self, query, file, remote = False, **kwargs ):
        r = "" if remote else "LOCAL" #TESTME
        
        query += " INTO %s DATA OUTFILE " % r #TESTME
        query += self.quote_string(file)
        
        cursor = self.mysql_query(query, **kwargs) #TODO: port kwargs to PHP
        cursor.close()
        
        return self.connection.affected_rows()
    

    def insert_from_file (self, table, file, remote = False, **kwargs ):
        r = "" if remote else "LOCAL" #TESTME

        query = ""
        query += " LOAD %s DATA INFILE " % r #TESTME
        query += self.quote_string(file)
        query += " INTO TABLE %s " % table
        
        cursor = self.mysql_query(query, **kwargs) #TODO: port kwargs to PHP
        cursor.close()
        
        return self.connection.affected_rows()
    
    
    def close(self):
        if self._update_cursor:
            try:
                self._update_cursor.close()
            except Exception as e:
                self._trace(__function__(), "failed to close mysql cursor: %s" % e)
                #XXX: do we really not care? can we go on? could there have been a commit pending?
        
        if self.connection:
            try:
                self._trace(__function__(), "closing mysql connection")
                self.mysql_close()
            except Exception as e:
                self._trace(__function__(), "failed to close mysql connection: %s" % e)
                #XXX: do we really not care? can we go on? could there have been a commit pending?
        
        return super(MySQLGlue, self).close()

     
    @staticmethod
    def new_client_connection(graphname, host = False, port = False ):
        return MySQLGlue( ClientTransport(host, port), graphname ) #FIXME: PORT graphname stuff to PHP!
    

    @staticmethod
    def new_slave_connection(command, cwd = None, env = None ):
        return MySQLGlue( SlaveTransport(command, cwd, env), None )
    
    
    def dump_query (self, sql ):
        print "*** %s ***" % sql
        
        res = self.mysql_query( sql )
        if ( not res ): return False
        
        c = self.dump_result( res )
        res.close()
        
        return c
    
    
    def dump_result (self, res ):
        keys = None
        c = 0
        
        print ""
        while True:
            row = _fetch_dict(res)
            if not row: break
            
            if keys is None :
                s = ""
                for k in row.keys():
                    s += k
                    s += "\t"
                
                
                print s
            
            s = ""
            for v in row:
                    s += v
                    s += "\t"
            
            print s
            c += 1
        
        
        print "-----------------------------"
        print "%i rows" % c
        
        return c
    

