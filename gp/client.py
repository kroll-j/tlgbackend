"""Graph Processor Client Library by Daniel Kinzler
Translated from PHP to Python by Philipp Zedler
Copyright (c) 2011 by Wikimedia Deutschland e.V.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
  * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.
  * Redistributions in binary form must reproduce the above copyright
    notice, this list of conditions and the following disclaimer in the
    documentation and/or other materials provided with the distribution.
  * Neither the name of Wikimedia Deutschland nor the
    names of its contributors may be used to endorse or promote products
    derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY WIKIMEDIA DEUTSCHLAND ''AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL WIKIMEDIA DEUTSCHLAND BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


NOTE: This software is not released as a product. It was written primarily for
Wikimedia Deutschland's own use, and is made public as is, in the hope it may
be useful. Wikimedia Deutschland may at any time discontinue developing or
supporting this software. There is no guarantee any new versions or even fixes
for security issues will be released.

This version of the Graph Processor Client Library
is a Python interface to a GraphServ or GraphCore instance. 

@author    Daniel Kinzler <daniel.kinzler@wikimedia.de>
@author    Philipp Zedler <zedler@itp.tu-berlin.de> (translation)
@copyright 2011, Wikimedia Deutschland

@package   WikiTalk #? Stimmt das?

"""

import re
import os
import socket
import time
import subprocess
import inspect
import types

LINEBREAK = "\r\n"
"""Linebreak to use when talking to GraphServ or GraphCore instances.
   This is \\r\\n per spec. but \\n alone should also work."""

PORT = 6666
"""Default GraphServ port"""

CLIENT_PROTOCOL_VERSION = 4
"""Implemented GraphServ protocol version. May be used to determin which
   features are supported. Is not used to validate the peer's protocol version,
   see MIN_PROTOCOL_VERSION and MAX_PROTOCOL_VERSION for that."""

MIN_PROTOCOL_VERSION = 2.0
"""Minimum GraphServ protocol version. If GraphServ (resp. GraphCore)
   reports a lower protocol version, the connection will be aborted."""

MAX_PROTOCOL_VERSION = 4.99
"""Maximum GraphServ protocol version. If GraphServ (resp. GraphCore)
   reports a higher protocol version, the connection will be aborted."""


def __function__ (shift = 1): #XXX: wtf?
    caller = inspect.stack()[shift]
    return caller[3]

class gpException(Exception):
    """Base class for exceptions in this module."""
    def __init__(self, msg):
        """
        @param msg: the message to be displayed in case of an exception
        """
        self.msg = msg

    def __str__(self):
        """Show type and name in the error message."""
        return type(self).__name__ + ": " + self.msg

    def getMessage(self):
        """Returns the error message."""
        return self.msg


class gpProcessorException(gpException):
    """Exceptions for errors reported by the remote grap database"""
    def __init__(self, status, msg, command=False ):
        if command:
            msg = msg + " Command was %s" % command
        gpException.__init__(self, msg)
        self.command = command
        self.status = status
        #? self.status wird nirgendwo ausgegeben.


class gpProtocolException(gpException):
    """Exception for the communication with the remote graph database."""
    pass


class gpClientException( gpException ):
    """Exception when gpClient encounters a problem on the client side."""
    pass


class gpUsageException( gpClientException ):
    """Exception raised when gpClient is used incorrectly."""
    pass





class DataSource (object):
    """Represents an interator of rows in a tabular data set.

    Data sources are used in the gpClient framework to represent origin of
    a data transfer. Typically, a data source is used to provide data to a
    GraphCore command, such as add_arcs. Derived classes must implement the
    next() method, which returns one row of data after another.

    """
    def __iter__(self):
        """Return the iterator object. Required for the iterator protocol."""
        return self

    def next( self ):
        """Returns the next row.

        The row is represented as an indexed array. Successive calls on the
        same data source should return rows of the same size, with the same
        array keys.
        @return an array representing the next row.
    
        """
        raise NotImplementedError( "`next()' not implemented by %s" % self.__class__ )
    
    def close( self ):
        """ Close the data source and free resources allocated by this object.

        close() should always be called when a data source is no longer
        needed, usually on the same level as the data source object was
        created. After close() has been called on a data source object,
        the behavior of calling next() on that object is undefined.
    
        """
        pass    # noop
        
    def drain( self ): #TODO: PORT TO PHP
        """ Drains the source and returns all data rows as an array.
        """
        
        data = []
        
        for r in self:
            data.append(r)
            
        self.close()
        return data


class NullSource( DataSource ):
    """An empty data source."""
    
    def next( self ):
        """Stop the iteration."""
        raise StopIteration()
    
    instance = None
    """kind of singleton instance of NullSource"""

NullSource.instance = NullSource()
"""A global variable of the module containing a NullSource instance.

It hould be used in order to avoid the costs of crating new instaces
which are not necessary for this class.

"""


class ArraySource( DataSource ):
    """A data source that iterates over an array.

    This is useful to use programmatically generated data as the input
    to some GraphCore command.

    The ArraySource maintains a current index pointing into the data
    array. Every call to next() increments that index to the next row.

    """

    def __init__( self, data ):  
        """ Initializes a ArraySource from the table contained in data.
   
        @param data: a list of lists or tuples, each representing a
               row in the data source. If the list contains integers
               or strings, they are wrapped and returned as one-tuples.

        """
        self.data = data
        self.data_length = len(data)
        self.index = 0
         
    def next( self ):
        """Return the next row of the list provided to the constructor."""
        if self.index < self.data_length:
            row = self.data[self.index]
            self.index = self.index + 1
            
            if not isinstance(row, (list,tuple)):
                if not isinstance(row, (str, unicode, int, long)):
                    raise gpUsageException("data must consist of strings or integers")
                    
                row = (row, )
            return row
        else:
            raise StopIteration()
 
    def makeSink(self):
        """Returns a new instance of ArraySink.

        The sink can be used to write to and to fill the data list of
        this ArraySource.
   
        """ 
        return ArraySink(self.data)
     

class LimitedSource( DataSource ): #TODO: PORT to PHP
    """A data source that wraps another data source to limit the number
    of rows returned from it.

    This is useful to limit the number of arcs transmitted graphserv in
    a single command.
    """

    def __init__( self, src, limit ):  
        """ Initializes a LimitedSource using the given original data source.
   
        @param src: a DataSource object
        @param limit: the number of rows to return.
        """
        
        self.source = src
        self.limit = limit
        self.index = 0
         
    def next( self ):

        """Return the next row of the DataSource provided to the constructor."""
        
        if self.index < self.limit:
            row = self.source.next()
            self.index = self.index + 1
            
            return row
        else:
            raise StopIteration()
            
    def limit_reached( self ):
        """ returns True if next() has already been called sucessfully as many times
            as allowed by the limit parameter passed to the constructor. After
            iterating over this LimitedSource instance (i.e. after StopIteration()
            has been thrown by next()), this method may be used to determine
            whether there may be more data in the original data source. If 
            iteration was terminated but limit_reached() returns false, then the
            original source was depleted and there is no more data available from it.
        """
        
        return ( self.index >= self.limit )
 
class PipeSource( DataSource ):
    """Data source based on a file handle.

    Each line read from the file handle is interpreted as (and converted
    to) a data row.
    Note: calling close() on a PipeSource does *not* close the
    underlying file handle. The idea is that the handle should be closed
    by the same code that also opened the file.

    """
    def __init__( self, hin ):
        """Initializes a new PipeSource
    
        @param resource hin a handle of an open file that allows read
        access, as returned by file() or ... #? translate fsockopen()!
    
        """
        self.hin = hin
        
    def next(self):
        """Returns the next line from the file handle (using readline).

        The line is split using Connection.splitRow() and the result is
        returned as the next row.
   
        @return array the next data row, extracted from the next line
                read from the file handle.
   
        """ 
        s = self.hin.readline()
        s = s.strip()
        if s:
            row = Connection.splitRow( s )
            return row
        else:
            raise StopIteration()


class FileSource( PipeSource ):
    """Data source based on reading from a file.

    Extends PipeSource to handle an actual local file.
 
    Note: calling close() on a FileSource *does* close the
    underlying file handle. The idea is that the handle should be closed
    by the same code that also opened the file.

    """    
    def __init__(self, path, mode='r'):
        """Creates a data source for reading from the given file.

        The file is opened using file(path, mode).
    
        @param string path the path of the file to read from
        @param string mode (default: 'r') the mode with which the file
               should be opened.
        @throws gpClientException if file() failed to open the file
                given by path.
    
        """
        self.mode = mode
        self.path = path
        
        try:
            handle = file( self.path, self.mode )
        except IOError:
            raise gpClientException( "failed to open " + self.path )
        PipeSource.__init__(self, handle)
         
    def close(self):  
        """Close the file handle."""
        self.hin.close()
         
     



class DataSink(object): #abstract
    """Abstract base class for "data sinks".

    The gpClient framework uses data sink objects to represent the
    endpoint of a data transfer. That is, a data sink accepts one row
    of tabular data after another, and handles them in some way.
    How the row is processed is specific to the concrete implementation. 
    
    """
    def putRow(self, row):
        raise NotImplementedError( "`putRow()' called in abstract class" )
    
    def flush(self):
        """Write buffered data.

        In case any output has been buffered (or some other kind of
        action has been deferred), it should be written now (resp.
        deferred actions should be performed and made permanent).
    
        The default implementation of this method does nothing. Any
        subclass that applies any kind of buffereing to the output
        should override it to make all pending changes permanent.
    
        """
        
        pass
    
    def close(self):
        """Close this data output and releases allocated resources.

        The behavior of calls to putRow() is undefined after close()
        was called on the same object.
    
        The default implementation of this method calls flush(). Any
        subclass that allocates any external resources should override
        this method to release those resources.
   
        """ 
        
        self.flush()
         
     
class NullSink( DataSink ):
    """A data sink that simply ignores all incoming data."""
        
    def putRow(self, row):
        pass
        
    def flush(self):
        pass

    instance = None

NullSink.instance = NullSink()
"""A global variable of the module containing a NullSink instance.

It hould be used in order to avoid the costs of crating new instaces
which are not necessary for this class.

"""

class ArraySink(DataSink):
    """A data sink that appends each row to a data array.

    This is typically used to make the data returned from a GraphCore
    command available for programmatic processing. It should however not
    be used in situations where large amounts of data are expected to be
    returned.

    """
    def __init__(self, data=None):     #? data war Zeiger!
        """Initializes a new ArraySink.
    
        @param array data (optional) an array the rows
               should be appended to. If not given, a new array will be
               created, and can be accessed using the getData() method.
    
        """
        
        if data is None:
            #NOTE: don't use [] as a default param, otherwise we'll be 
            #      using the same list instance for all calls!
            data = []
        
        self.data = data
         
    def putRow(self, row):
        """Appends the given row to the table maintained by this ArraySink.

        The data can be accessed using the getData() method.

        """
        
        self.data.append(row)
         
    def getData(self):
        """Returns the array that contains this ArraySink's tabular data.

        This method is typically used to access the data collected by this
        data sink.
    
        """
        return self.data
         
    def makeSource(self):
        """Return a new instance of ArraySource.

        It may be used to read the rows from the array of tabular data
        maintained by this ArraySink.
    
        """
        return ArraySource(self.data)
         
    def getMap(self):
        """Return the maintained tabular data as an associative array.

        This only works for two column data, where each column is
        interpreted as a pair of key and value. The first column is
        used as the key and the second column is used as the value.
     
        @rtype:  dictionary
        @return: an associative array created under the assumption
                 that the tabular data in this ArraySink consists of
                 key value pairs.

        """
        return pairs2map(self.data)


class PipeSink (DataSink):
    """Data sink based on a file handle.

    Each data row is written as a line to the file handle.

    Note: calling close() on a PipeSink does *not* close the
    underlying file handle. The idea is that the handle should be closed
    by the same code that also opened the file.

    """
    
    def __init__(self, hout, linebreak=None):
        """Initializes a new pipe sink with the given file handle.

        @param resource $hout a file handle that can be written to, such as 
               returned by fopen or fsockopen.
        @param string $linebreak character(s) to use to separate rows in the
               output (default: LINEBREAK)
    
        """
        if not linebreak:
            linebreak = LINEBREAK
        self.hout = hout
        self.linebreak = linebreak
         
    def putRow(self, row):
        """Writes the given data row to the file handle.

        Connection.joinRow() is used to encode the data row into a
        line of text. PipeTransport.send_to() is used to write the
        line to the file handle.
     
        Note that the rows passed to successive calls to putRow() should
        have the same number of fields and use the same array keys.
     
        @type row:  list/tuple of int/str types
        @param row: representation of a data row.
    
        """
        s = Connection.joinRow(row)
        PipeTransport.send_to(self.hout, s + self.linebreak)
         
    def flush(self):
        """Flushes any pending data on the file handle (using fflush)."""
        self.hout.flush()
         
     
class FileSink (PipeSink):
    """Data sink based on writing to a file.

    Extends PipeSink to handle an actual local file.

    Note: calling close() on a FileSink *does* close the underlying
    file handle. The idea is that the handle should be closed by the
    same code that also opened the file.

    """
    
    def __init__(self, path, append=False, linebreak=None):
        """Creates a new FileSink around the given file.

        The file given by path is opened using file().
     
        @param string path the path to the local file to write to.
        @param boolean append whether to append to the file, or override it
        @param string linebreak character(s) to use to separate lines in the
               resulting file (default: os.linesep)
        @throws gpClientException if the file could not be opened.
    
        """
        if append == True:
            self.mode = 'a'
        elif append == False:
            self.mode = 'w'
        else:
            self.mode = append
        if not linebreak:
            linebreak = os.linesep
        self.path = path
        try:
            h = file(self.path, self.mode)
        except Error:
            raise gpClientException( "failed to open %s" % self.path )
        PipeSink.__init__(self, h, linebreak )
         
    def close(self):
        """closes the file handle (after flushing it)."""
        PipeSink.close(self)
        self.hout.close()





class Transport(object): # abstract
    """Abstract base class of all transports used by the gpClient framework.

    A transport abstracts the way the framework communicates with the
    remote peer (i.e. the instance of GraphServ resp. GrahCore). It
    also implements to logic to connect to the remote instance.

    """

    def __init__(self, *otherArguments):
        """The constructor."""
        self.closed = False
        self.debug = False
        self._eof = False
    
    def trace(self, context, msg, obj='nothing878423really'):
        """Trace an error."""
        if ( self.debug ):
            if obj != 'nothing878423really':
                msg = msg + ': ' + re.sub('\s+', ' ', str(obj))
                
            print "[Transport] %s: %s" % (context, msg)
    
    def isClosed(self):
        """Return True if this Transport is closed, e.g. with close()"""
        return self.closed
    
    def close(self):
        """Closes this Transport

        Disconnect from the peer and free any resources that this object
        may have allocated. After close() has been called, isClosed()
        must always return True when called on the same object.
        The default implementation just marks this object as closed.
    
        """
        self.closed = True
         
    def connect(self):
        """Connects this gptransport to its peer.

        Its peer is the remote instance of GraphServ resp. graphCore.
        The information required to connect is typically provided to the
        constructor of the respective subclass.

        """
        raise NotImplementedError("`connect()' not implemented by %s" % self.__class__)
    
    def send(self, s):
        """Sends a string to the peer.

        This is the an operation of the line based communication protocol.
    
        """
        raise NotImplementedError("`send()' not implemented by %s" % self.__class__)
    
    def receive(self):
        """Receives a string from the peer.

        This is the a operation of the line based communication protocol.

        """
        raise NotImplementedError("`receive()' not implemented by %s" % self.__class__)
    
    def eof(self):
        """True after detection of end of data stream from the peer"""
        return self._eof
    
    def make_source(self):
        """Creates an instance of DataSource

        for reading data from the current position in the data stream
        coming from the peer.
    
        """
        raise NotImplementedError("`make_source()' not implemented by %s" % self.__class__)
    
    def make_sink(self):
        """Create an instance of DataSink

        for writing data to the data stream going to the peer.
    
        """
        raise NotImplementedError("`make_sink()' not implemented by %s" % self.__class__)
 
    def checkPeer(self): 
        """Attempts to check if the peer is still responding. 

        A static function.
        The default implementation does nothing.

        """
        pass   # noop
          
    def setDebug(self,debug):
        """Sets the debug mode on this transport object.

        When debugging is enabled, details about all data send or
        received is deumpted to stdout. 
    
        """
        self.debug = debug


class PipeTransport(Transport): # abstract
    """Abstract base for file handle based implementations of Transport."""

    def __init__(self):
        self.hout = None
        self.hin = None
        
        self.out_chunk_size = None
        
        Transport.__init__(self)
    
    @staticmethod
    def send_to(hout, s, chunk_size = None):
        """Utility function for sending data to a file handle.

        This is essentially a wrapper around file.write(), which makes sure
        that s is written in its entirety. After s was written out using
        file.write(), file.flush() is called to commit all data to the peer.

        @param resource hout: the file handle to write to
        @param string s: the data to write
        @raise gpProtocolException if writing fails.
    
        """
        try:
            if chunk_size: # write small chunks
                i = 0
                while i <= len(s):
                    hout.write(s[i:i+chunk_size])
                    hout.flush()            # try to write the buffer
                    i += chunk_size
            else: # write all at once
                hout.write(s)
                hout.flush()            # try to write the buffer
        except IOError:
            raise gpClientException(
              "failed to send data to peer, broken pipe! "
              + "(Writing to the file failed.)")
        except:
            raise gpClientException( "failed to send data to peer, broken pipe! "
              + "(A strange error occured.)")
            raise
         
    def send(self, s):
        """Sends the given data string to the peer

        by writing it to the output file handle created by the connect()
        method. Uses PipeTransport.send_to() to send the data.
    
        """
        return PipeTransport.send_to(self.hout, s, self.out_chunk_size)
    
    def receive(self):
        """Receives a string of data from the peer
        by reading a line from the input file handle created by the
        connect() method. Uses readline to send the data. 
    
        @todo: remove hardcoded limit of 1024 bytes per line!
        #? Here any problem?
    
        """
        re = self.hin.readline()
        if not re:
            self._eof = True
        return re

    def setTimeout(self, seconds):
        """Sets a read timeout on input file handle
    
        which is created by the connect() method.
    
        """
        self.hin.settimeout(seconds)

    def make_source(self):
        """Returns a new instance of PipeSource

        and this reads from the input file handle created by the
        connect method().
    
        """
        return PipeSource( self.hin )
         
    def make_sink(self):
        """Returns a new instance of PipeSink

        that writes to the output file handle created by the connect method().
   
        """ 
        return PipeSink(self.hout)

    def close(self): #TODO: port to PHP!
        self.trace(__function__(), "closing pipes")

        if self.hin:
            try:
                self.hin.close()
            except:
                pass
    
        if self.hout:
            self.hout.flush()
            
            try:
                self.hout.close()
            except:
                pass
    
        Transport.close(self)

class ClientTransport(PipeTransport):
    """Communicate with a remote instance of GraphServ over TCP.

    An implementation of PipeTransport.
    @var host
    @var port
    @var socket = False
    """
    
    def __init__(self, host='localhost', port=PORT): #OK
        """Initialize a new instance of ClientTransport.

        Responsable for a connection with GraphServ.

        @param string host (default: 'localhost') the host the GraphServ
               process is located at
        @param int port (default: PORT) the TCP port the GraphServ
               process is listening at
    
        """
        self.port = port
        self.host = host
        #FIXME: PORT removal of self.graphname to php
        self.socket = False
        PipeTransport.__init__(self)
         
    def connect(self):
        """Connects to a remote instance of GraphServ

        using the host and port provided to the constructor.
        If the connection could be established, opens the graph
        specified to the constructor.
        B{#? In PHP werden hier noch $errno und $errstr uebergeben. Philipp.}
     
        @throws gpProtocolException if the connection failed or another
                communication error ocurred.
    
        """
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.socket.connect((self.host, self.port))
            #XXX: configure timeout?
        except socket.error as (value, message):
            raise gpProtocolException(
              "failed to connect to %s:%s: %s %s" % (self.host, self.port, value, message) )
        
        self.hin = self.socket.makefile("r")
        self.hout = self.socket.makefile("w")
        
        return True
         
    def close(self):
        """Closes the transport.

        Disconnect the TCP socket to the remote GraphServ instance (using
        fclose). Subsequent calls to close() have no further effect.
        @throws gpProtocolException if the connection failed or another
        communication error ocurred.
    
        """

        PipeTransport.close(self)

        if not self.socket:
            return False
        
        self.trace(__function__(), "closing socket")
                
        #manual sais: use shutdown() before close() to close socket in a "timely fashion".
        try:
            self.socket.shutdown(socket.SHUT_RDWR) 
        except socket.error as e:
            self.trace(__function__(), "socket.shutdown() failed: %s" % e)
            
        try:
            self.socket.close()
        except socket.error as e:
            self.trace(__function__(), "socket.close() failed: %s" % e)
        
        self.closed = True
     

class SlaveTransport(PipeTransport):
    """A transport implementation for communicating

    with a GraphCore instance running in a local child process (i.e. as
    a slave to the current PHP script).
    
    @var process
    @var command

    """
    
    def __init__(self, command, cwd=None, env=None):
        """Initialize a new instance of SlaveTransport.

        Launch a slave instance of GraphCore.
     
        @param mixed command the command line to start GraphCore.
               May be given as a string or as an array. If given as a
               string, all parameters must be duely escaped #?. If given as
               an array, command[0] must be the path to the GraphCore
               executable. See Slavetransport.makeCommand() for more
               details.
        @param string cwd (default: None) the working dir to run the
               slave process in. Defaults to the current working 
               directory. #? Check!
        @param int $env (default: null) the environment variables to
               pass to the slave process. Defaults to inheriting the PHP
               script's environment.
    
        """
        self.command = command
        self.cwd = cwd
        self.env = env
        self.process = None
        PipeTransport.__init__(self)
    
    @staticmethod
    def makeCommand(command):
        """Utility function for creating a valid command line.
        
        It is called before executing a program as a child process.
     
        @type command:  str or list or tuple
        @param command: the command, including the executable and any
               parameters. If given as a string, all parameters must be
               duely escaped. #? If given as an array, command[0] must be
               the path to an executable.
        @rtype:  str
        @return: A valid command line. The first part of
                 the command is the executable, any following parts are
                 passed as arguments to the executable.
        @raise: gpClientException if the command did not point to a readable,
                executable file.
    
        """
        if not command:
            raise Exception('empty command given')
        
        path = None
        if isinstance(command, (list, tuple)):
            for i in command:
                if i == 0:
                    cmd = command[i]
                    # In the php-Version, escapeshellcmd is called here.
                    # Python claims to handle arguments securely.
                    path = command[i]
                else:
                    cmd = cmd + ' ' + str(command[i])
                    # Here the same with escapeshellarg
        else:
            m = re.search(
              '!^ *([-_a-zA-Z0-9.\\\\/]+)( [^"\'|<>]$|$)!', command)
            if m:
                path = m.group(1)
            cmd = command.strip()
             
        
        if path:
            if not os.path.exists(path):
                raise gpClientException('file does not exist: ' + path)
            if not os.access(path, os.R_OK):
                raise gpClientException('file is not readable: ' + path)
            if not os.access(path, os.X_OK):
                raise gpClientException('file is not executable: ' + path)
        
        return cmd
    
    def connect(self):
        """Connects to the slave instance of GraphCore

        launched using the command provided to the constructor.
        proc_open() is used to launch the child process.
     
        @throws gpClientException if the command executable could not be found.
        @throws gpProtocolException if the child process could not be launched.
     
        @todo handle output to stderr!
        @todo get rid of the "wait 1/10 of a second and check" hack
    
        """
        cmd = self.makeCommand(self.command)
        try:
            #pexpect.spawn(cmd,cwd=self.cwd,env=self.env) 
            # pty.spawn(cmd, self.hin, self.hout)
            self.process = subprocess.Popen(cmd, cwd=self.cwd, env=self.env, stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.STDOUT) 
            
            self.hin = self.process.stdout
            self.hout = self.process.stdin
            
        except Exception as ex:
            self.trace(__function__(), "failed to execute %s" % str(self.command))
            raise gpProtocolException("failed to execute %s" % str(self.command))
        
        self.trace(__function__(), "executing command "
          + str(self.command) + " as %s" % str(self.process))
                
        self.trace(__function__(), "reading from %s" % str(self.hin))
        self.trace(__function__(), "writing to %s" % str(self.hout))
        
        #time.sleep(0.1)
        # XXX: NASTY HACK!
        # wait 1/10th of a second to see if the command actually starts
        
        self.checkPeer()
        
        return True

    @staticmethod
    def send_to(a,b):
        raise NotImplementdError(
          "send_to should not be called for a SlaveTransport object.")
    
    def close(self):
        """Close transport by terminating slave process using Popen.terminate()."""
        
        #PipeTransport.close(self) #XXX: call parent to close pipes?!
        
        if not self.process:
            return False
        
        self.process.terminate()
        self.process.wait()
        
        self.process = False
        self.closed = True
    
    def checkPeer(self):
        """Check if slave process is still alive using Popen.poll().

        @throws gpProtocolException if the slave process is dead.

        """
        
        code = self.process.poll()
        if code is not None:
            raise gpProtocolException('slave process is not running! exit code ' + code)
            

class Connection(object):
    """This class represents an active connection to a graph.

    It can be seen
    as the local interface to the graph that allows the graph to be
    queried and manipulated, using the command set specified for GraphCore
    and GraphServ. The communication with the peer process that manages
    the actual graph (a slave GraphCore instance or a remote GraphCore
    server) is performed by an instance of the appropriate subclass of
    Transport.
    
    Instances of Connection that use the appropriate transport can be
    created conveniently using the static factory methods called
    new_xxx_connection.
    
    Besides some methods for managing the connection and some utility
    functions, Connection exposes the GraphCore and GraphServ command
    sets. The commands are exposed as "virtual" methods: they are not
    implemented explicitely, instead, the __getattr__() method is used to map
    method calls to GraphCore commands. No local checks are performed on
    the command call, so it's up to the peer to decide which commands
    exist. Note that this means that a Connection actually exposes more
    commands if it is connected to a GraphServ instance (simply because
    the peer then supports more commands).
    
    The mapping of method calls to commands is performed as follows:
    
      * underscores are converted to dashes: the method add_arcs
        corresponds to the add-arcs command in GraphCore. 
    
      * any int or string parameters passed to the method are passed on
        to the command call, in the order they were specified.
    
      * parameters that are instances of DataSource will be used to pass
        a data set to the command. That is, rows from the data source are
        passed to the command as input.
    
      * parameters that are instances of DataSink will be used to handle
        any data the command outputs. That is, rows from the command's
        output data set will be passed to the data sink, one by one.
    
      * parameters that are arrays are wrapped in a new instance of
        ArraySource and used as input for the command, as described
        above. This is convenient for passing data directly to the
        command.
    
      * parameters given as None or False are ignored.
    
      * other types of arguments trigger a gpUsageException
    
    A command called in this way, using its "plain" method counterpart,
    always returns the status string from the peer's response upon
    successful execution. The status may be "OK" or "NONE". Any failure on
    the server side triggers a gpProcessorException. Any output of the
    command is passed to the DataSink that was provided as a parameter
    (or ignored if no sink was provided).
    
    However, modifiers can be attached to the method name to cause the
    command's outcome to be treated differently:
    
      * if the method name is prefixed with "try_", no
        gpProcessorException are thrown. Instead, errors reported by the
        peer cause the method to return false. The cause of the error may
        be examined using the getStatus() and getStatusMessage() methods.
        Note that other exceptions like gpProtocollException or
        gpUsageException are still thrown as usual.
    
      * if the method name is prefixed with "capture_", the command's
        output is collected and returned as an array of arrays,
        representing the rows of data. If the command fails, a
        gpProcessorException is raised, as usual (or, if try_ is also
        specified, the method returns false).
    
      * if the method name is suffixed with "_map" AND prefixed with
        "capture_", the command's output is collected and returned as an
        associative array. This is especially useful for commands like
        "stats" that provide values for set of well known properties.
        To build the associative array, rows from the output are
        interpreted as a key-value pairs. If the _map suffix is used
        without the capture_ prefix, a gpUsageException is raised.
    
    Modifiers can also be combined. For instance, try_capture_stats_map
    would return GraphCore stats as an associative array, or null of the
    call failed.
    
    Additional modifiers or extra virtual methods can be added by
    subclasses by overriding the __getattr__() method or by registering
    handlers with the addCallHandler() or addExecHandler() methods.

    """

    
    
    def __init__( self, transport=None, graphname = None ):
        """Initialize a new connection with the given instance of
        Transport.
    
        Note: Instances of Connection that use the appropriate
        transport can ba e created conveniently using the static
        factory methods called new_xxx_connection.
        
        @rtype: None

        """
        self.transport = transport
        """The transport used to communicate with the peer that manages the
        actual graph."""
        #? Should the type of transport be checked?
        #? EAFP: no. (easier to ask for forgiveness than permission)        
        self.tainted = False
        """If true, the protocol session is "out of step" and no further
        commands can be processed."""
        self.status = None
        """The status string returned by the last command call."""
        self.statusMessage = None
        """The status message returned by the last command call."""
        self.response = None
        """The response from the last command call, including the status
        string and status message."""
        self.call_handlers = []
        """call handlers, see addCallHandler()"""
        self.exec_handlers = []
        """Exec handlers, see addExecHandler()."""
        self.allowPipes = False
        """If peer-side input and output redirection should be allowed. For
        security reasons, and to avoid confusion, i/o redirection is disabled
        per default."""
        self.strictArguments = True
        """Whether arguments should be restricted to alphanumeric strings.
        Enabled by default."""
        self.__command_has_output = None
        """wheather the command performed by execute() has generated
        output"""
        self.debug = False
        """Debug mode enables lots of output to stdout."""
        self.graphname = graphname #TODO: port this to PHP
        
        self._protocol_version = None
 
    def connect(self):
        """ Connect to the peer. 
    
        For connecting, this method relies solely on the transport
        instance, which in turn uses the information passed to its
        constructor to establish the connection.
    
        After connecting, this method calls checkProtocolVersion()
        to make sure the peer speaks the correct protocol version.
        If not, a gpProtocolException is raised.
        
        @rtype: None
   
        """
        self.transport.connect()
        self.checkProtocolVersion()
        
        if self.graphname: #TODO: port this to PHP
            self.use_graph( self.graphname )
         
    def addCallHandler( self, handler ): #OK
        """Register a call handler.
        
        The handler will be called before __getattr__ interprets a
        method call as a GraphCore command, and can be used to add
        support for additional virtual methods or extra modifiers.
    
        The handler must be a callable with the following signature:
        handler(connection, {'command': ..., 'args': ..., 'source': ...,
                'sink': ..., 'capture': ..., 'result': ...})
        i.e. it accepts the varible connection
        (this Connection instance) and a dictionary with the
        following keys:
    
        * 'command' a reference to the command name, as a string, with
          the try_, capture_ and _map modifiers removed.  
        * 'arguments' a reference to the argument array, unprocessed, as 
          passed to the method.
        * 'source' a reference to a DataSource (or None), may be 
          altered to change the command's input.
        * 'sink' a reference to a DatSink (or null), may be altered to 
          change the output handling for the command.
        * 'capture' a reference to the capture flag. If true, output 
          will be captured and returned as an array.
        * 'result' the result to return from the method call, used 
          only if the handler returns false.
        * If the handler returns false, the value of 'result' will be 
          returned from __getattr__ and no further action is taken.
        
        
        @rtype: None
        
        """
        self.call_handlers.append( handler )
         
    def addExecHandler(self, handler): #OK
        """Register a call handler.
    
        # handler(connection, {'command': ..., 'source': ...,
        #         'sink': ..., 'has_output': ..., 'status': ...})
        #? I don't understand the first argument. It's not used in gpMySQL.php.    
    
        The handler will be called before __getattr__ passes a command
        to the execute() method, and can thus be used to add support
        for additional "artificial" commands that use the same parameter
        handling as is used for "real" GraphCore commands.
        
        The handler must be a callable that accepts the varible connection
        (this Connection instance) and a dictionary with the
        following keys:
    
          * 'command' a refference to the command, as an array.
            The first field is the command name,
            the remaining fields contain the parameters for the command.
          * 'source' a reference to a gpDatSource (or null), may be altered to 
            change the command's input.
          * 'sink' a reference to a gpDatSink (or null), may be altered to 
            change the output handlking for the command.
          * 'status' the commands return status, used of the handler
            returns false.
         
        If the handler returns false, the value of $status will be used as the
        command's result, and no command will be sent to the peer. The value
        of $status is treated the same way the status returned from the peer is:
        e.g. a gpProcessorException is thrown if the status is "FAILED", etc.
        Also, modifiers like capture_ are applied to the output in the same way
        as they are for "normal" commands.
        
        """
        self.exec_handlers.append(handler)
         
    
    def getStatus(self): #? Not consistent with command call
        """Return the status string that resulted from the last command call.

        The status string is 'OK' or 'NONE' for successfull calls, or 'FAILED',
        'ERROR' or 'DENIED' for unsuccessful calls. Refer to the GraphCore and
        GraphServ documentation for details.
        
        @rtype:  str
        @return: the status string
    
        """
        return self.status
         
    
    def isClosed(self): # OK
        """Tell if the connection is closed.

        Returns true if close() was called on this connection, or it was closed for
        some other reason. No commands can be called on a closed connection.
    
        """
        return self.transport.isClosed()
         
    
    def getStatusMessage(self): #? Check if consistent with commandcall
        """Return the status message that resulted from the last command call.

        The status message is the informative message that follows the status
        string in the response from a command call. It may be useful for human
        eyes, but should not be processed programmatically.
    
        """
        return self.statusMessage
         
    
    def getResponse(self): #? Ceck command call.
        """Return the response that the last command call evoked.

        This consists of the status string and the status message.
    
        """
        return self.response
         
    
    def _trace(self, context, msg, obj_type='nothing878423really'): #halbwegs OK
        """Print messages to stdout when debug mode is enabled."""
        if self.debug:
            if obj_type != 'nothing878423really' and obj_type != type(None):
                #? and... appears not in the php-version
                #? introduced due to lack of ...?...:... in Python.
                #? Makes all other code shorter. Philipp.
                msg = msg + ': ' + re.sub('\s+', ' ', str(obj_type))
                #? Check if the substitution is really necessary!
            print "[gpClient] %s: %s" % (context, msg)
    
    def checkPeer(self): #? OK
        """Attempt to check if the peer is still alive."""
        self.transport.checkPeer()
          
    
    def setDebug(self, debug): #OK
        """Enable or disable the debug mode.

        In debug mode, tons of diagnostic information are written to stdout.
        @param bool debug 

        """
        self.debug = debug
        self.transport.setDebug(debug)
          
    
    def getProtocolVersion(self):
        """Return the protocol version reported by the peer."""
        
        if not self._protocol_version:
            self.protocol_version()
            self._protocol_version = self.statusMessage.strip()
        
        return self._protocol_version
          
    def supportsProtocolVersion(self, min_version, max_version = None): #TODO: port to PHP
        """returns True if the peer's protocol version is at least
           min_version and, if given, no grater than max_version.
        """
        
        version = self.getProtocolVersion()
        version = float(version)
        
        if min_version and version < min_version:
            return False
          
        if max_version and version > max_version:
            return False
            
        return True
        
    
    def checkProtocolVersion(self):
        """Can raise a gpProtocolException.

        It raises a gpProtocolException if the protocol version reported by the
        peer is not compatible with MIN_PROTOCOL_VERSION and MAX_PROTOCOL_VERSION.
    
        """
        
        version = self.getProtocolVersion()
        version = float(version)
        
        if version < MIN_PROTOCOL_VERSION:
            raise gpProtocolException(
                "Bad protocol version: expected at least "
                + str(MIN_PROTOCOL_VERSION)
                + ", but peer uses %s" % str(version) )
          
        if version > MAX_PROTOCOL_VERSION:
            raise gpProtocolException(
                "Bad protocol version: expected at most "
                + str(MAX_PROTOCOL_VERSION)
                + ", but peer uses %s" % str(version) )
          
    
    def ping(self): #? ?
        """Attempt to check if the peer is still responding."""
        theVersion = self.protocol_version()
        self._trace(__function__(), theVersion)
        
        return theVersion
         
    
    def __getattr__(self, name): # fast OK
        """Creates a closure that, when called, executes the command given
           as the attribute name on the peer instanceof graphcore resp graphserv.

            Refer to the class level documentation of Connection for details
            on how method calls are mapped to graphserv commands.
        """
        
        if re.search('_impl$', name):
            raise AttributeError("no such impl: %s" % name)
        
        #TODO: do command name normalization outside the closure!
        #TODO: allow named arguments!

        # A closure:
        def exec_command(*arguments):
            """Maps calls to undeclared methods on calls to graph commands.
        
            Refer to the class level documentation of Connection for details.
             
            @param arguments: the arguments passed to the method
           
            """ 
            cmd = re.sub('_', '-', name)
            cmd = re.sub('^-*|-*$', '', cmd)
            
            source = None
            sink = None
            row_munger = None #TODO: PORT TO PHP
            
            if re.match('^try-', cmd):
                cmd = cmd[4:]
                try_it = True
            else:
                try_it = False
                 
            
            if re.match( '^capture-', cmd ):
                cmd = cmd[8:]
                sink = ArraySink()
                capture = True
            else:
                capture = False
                 
            
            if re.search( '-map$', cmd ):
                if not capture:
                    raise gpUsageException(
                      "using the _map suffix without the capture_ prefix"
                      + " is meaningless" )
                cmd = cmd[:-4]
                map_it = True
            else:
                map_it = False
                 
            if re.search( '-value$', cmd ):
                if capture: 
                    raise gpUsageException( "using the _value suffix together with the capture_ prefix is meaningless" )
                
                cmd = cmd[:-6]
                val = True
            else:
                val = False
            
            result = None

            if self.call_handlers:
                handler_vars = {'command': cmd, 'arguments': arguments,
                                'source': source, 'sink': sink,
                                'capture': capture, 'result': result}
                                
                for handler in self.call_handlers:
                    go_on = handler( self, handler_vars )
                    if not go_on:   
                        return handler_vars['result']
                        
                cmd = handler_vars['command']
                arguments = handler_vars['arguments']
                source = handler_vars['source']
                sink = handler_vars['sink']
                capture = handler_vars['capture']
                result = handler_vars['result']
            
            command = [cmd]
    
            for arg in arguments:
                if isinstance(arg, (tuple, list, set)):
                    source = ArraySource(arg)
                elif type(arg) == types.GeneratorType:
                    source = ArraySource(arg)
                elif isinstance(arg, (DataSource, DataSink)):
                    if isinstance(arg, DataSource):
                        source = arg
                    elif isinstance(arg, DataSink):
                        sink = arg
                    else:
                        raise gpUsageException(
                          "arguments must be primitive or a DataSource"
                          + " or DataSink. Found %s" % str(type(arg)))
                elif not arg:
                    continue
                elif callable(arg):
                    row_munger = arg
                elif isinstance(arg, (str, unicode, int, long)):
                    command.append(arg)
                else:
                    raise gpUsageException(
                      "arguments must be objects, strings or integers. "
                      + "Found %s" % type(arg))
                      
            if try_it:
                catchThis = gpProcessorException 
                #XXX: catch more exceptions? ClientException? Protocolexception?
            else:
                catchThis = None
            
            try:
                do_execute = True
                self.__command_has_output = None
                
                if self.exec_handlers:
                    handler_vars = {'command': command, 'source': source, 
                                    'sink': sink, 'has_output': has_output,
                                    'status': status, 'row_munger': row_munger}

                    for handler in self.exec_handlers:
                        go_on = handler( self, handler_vars )
                        
                        if not go_on:
                            do_execute = False
                            break
                                
                    command = handler_vars['command']
                    source = handler_vars['source']
                    sink = handler_vars['sink']
                    has_output = handler_vars['has_output']
                    status = handler_vars['status']
                    row_munger = handler_vars['row_munger']

                if do_execute:
                    func = re.sub('-', '_', command[0] + '_impl')
                    if hasattr(self, func ):
                        args = command[1:]
                        args.append(source)
                        args.append(sink)
                        
                        f = getattr(self, func)
                        status = f( *args )
                    else:
                        status = self.execute(command, source, sink, row_munger = row_munger)
                         
                     
            except catchThis as e:
                return False
                 
            #note: call modifiers like capture change the return type!
            if capture:
                
                if status == 'OK' or status == 'VALUE':
                    if self.__command_has_output:
                        if map_it:
                            return sink.getMap()
                        else:
                            return sink.getData()
                    else:
                        return True
                         
                     
                elif status == 'NONE':
                    return None
                else:
                    return False
            else:
                if result:
                    status = result # from handler
                    
                if val:
                    if status == "VALUE" or status == "OK":
                        return self.statusMessage; #XXX: not so pretty
                    else:
                        raise gpUsageException( "Can't apply _value modifier: command " + command + " did not return a VALUE or OK status, but this: " + status )
                
                return status
                    
        setattr(self, name, exec_command) #re-use closure!

        # Return the closure.
        return exec_command 
         
    
    def execute(self, command, source=None, sink=None, row_munger=None):
        """ Applies a command to the graph, i.e. runs the command on the peer.
    
        Note: this method implements the protocol used to interact with the peers,
        based upon the line-by-line communication provided by the transport 
        instance. Interaction with the peer is stateless between calls to this
        function (except of course for the contents of the graph itself).
        
        If the command generates output, the instance variable
        __command_has_output will be set True, otherwise False.
        
        @type  command: mixed
        @param command: the command, as a single string or as an array
                        containing the command name and any arguments.
        @type  source: DataSource
        @param source: the data source to take the commands input from
                       (default: null)
        @type  sink: DataSink
        @param sink: the data sink to pass the commands output to
                     (default: null)
        @param row_munger: a callback function to be invoked for every row 
               copied (optional). The return value from the munger 
               replaces the original row. If the munger function returns 
               None or False, the row is skipped.
        @rtype:  string
        @return: the status string returned by the command
        @raise: gpProtocolException if a communication error ocurred while
                talking to the peer
        @raise: gpProcessorException if the peer reported an error
        @raise: gpUsageException if $command does not conform to the rules
                for commands. Note that self.strictArguments and
                self.alloPipes influence which commands are allowed.
        
        """
        self._trace(__function__(), "BEGIN")
        
        if self.tainted:
            raise gpProtocolException(
              "connection tainted by previous error!")
              
        if self.isClosed():
            raise gpProtocolException("connection already closed!")
            
        if self.transport.eof(): # closed by peer
            self._trace(__function__(),
                       "connection closed by peer, closing our side too.")
            self.close()
            self.tainted = True
            raise gpProtocolException("connection closed by peer!")
            
        if isinstance(command, (list, tuple, set)):
            if not command:
                raise gpUsageException("empty command!")
                
            c = command[0]
            if not isinstance(c, (str, unicode)): #? Less restrictive in php.
                raise gpUsageException(
                  "invalid command type: %s" % type(c).__name__)
                  
            if not self.isValidCommandName(c):
                raise gpUsageException("invalid command name: %s" % c)
                
            strictArgs = self.strictArguments

            if c == "set-meta" or c == "authorize": #XXX: ugly hack for wellknown commands
                strictArgs = False
            
            for c in command:
                if not isinstance(c, (str, unicode, int, long)):
                    raise gpUsageException(
                      "invalid argument type: %s" % type(c).__name__)
                      
                if self.allowPipes and re.match('^[<>]$', c):
                    strictArgs = False
                    # pipe, allow lenient args after that
                    
                if self.allowPipes and re.match('^[|&!:<>]+$', c):
                    continue
                    #operator
                    
                if not self.isValidCommandArgument(c, strictArgs):
                    raise gpUsageException("invalid argument: %s" % c)
            
            command = ' '.join("%s" % el for el in command)
        
        if not command:
            raise gpUsageException("command is empty!")
            
        command = command.strip()
        
        if command == "":
            raise gpUsageException("command is empty!")
            
        self._trace(__function__(), "command", command )
        
        if not self.isValidCommandString(command):
            raise gpUsageException("invalid command: %s" % command)
        
        if (not self.allowPipes) and re.search('[<>]', command):
            raise gpUsageException(
              "command denied, pipes are disallowed by allowPipes = false; "
              + "command: %s" % command);
        
        if source and (not re.search(':$', command)):
            command = command + ':'
        
        if (not source) and re.search(':$', command):
            source = NullSource.instance
        
        if source and re.search('<', command):
            raise gpUsageException(
              "can't use data input file and a local data source "
              + "at the same time! %s" % command)
        
        if sink and re.search('>', command):
            raise gpUsageException(
              "can't use data output file and a local data sink "
              + "at the same time! %s" % command)
        
        self._trace(__function__(), ">>> ", command)
        self.transport.send( command + LINEBREAK )
        self._trace(__function__(), "source", type(source))

        if ( source ):
            self._copyFromSource( source, row_munger = row_munger )
        
        rec = self.transport.receive()
        self._trace(__function__(), "<<< ", rec)

        
        if not rec:
            self.tainted = True;
            self.status = None;
            self.statusMessage = None;
            self.response = None;
            
            self._trace(__function__(),
                       "peer did not respond! Got value %s" % rec)
            self.transport.checkPeer()
            
            raise gpProtocolException(
              "peer did not respond! Got value %s" % str(rec))
        
        rec = rec.strip()        
        self.response = rec
        
        match = re.match('^([a-zA-Z]+)[.:!](.*?):?$', rec)
        if not match or not match.group(1):
            self.tainted = True
            self.close()
            raise gpProtocolException(
              "response should begin with status string like `OK`. Found: `"
              + rec + "'")
        
        self.status = match.group(1)
        self.statusMessage = match.group(2).strip()
        
        if self.status != 'OK' and self.status != 'NONE' and self.status != 'VALUE':
            raise gpProcessorException(
              self.status, self.statusMessage, command)
        
        self._trace(__function__(), "sink", type(sink))
        
        if re.search(': *$', rec ):
            if not sink:
                sink = NullSink.instance
                
            # note: we need to slurp the result in any case!
            self._copyToSink(sink, row_munger = row_munger)
        
            self.__command_has_output = True
        else:
            self.__command_has_output = False
             
        
        if self.transport.eof():        # closed by peer
            self._trace(__function__(),
                       "connection closed by peer, closing our side too.")
            self.close()
        
        return self.status
         
    
    

    def traverse_successors_without_impl(
      self, id, depth, without, without_depth, source, sink, row_munger = None):
        """Implements a 'fake' command traverse-successors-without

        which returns all decendants of onw nodes
        minus the descendants of some other nodes.
        This is a convenience function for a common case that
        could otherwise only be covered by implementing the
        set operation in php, or by using execute().

        This method should not be called directly.
        Instead, use the virtual method traverse_successors_without
        in the same way as normal commands are called.
        This include support for modifiers and flexible
        handling of method parameters.

        """
        if not without_depth:
            without_depth = depth
        return self.execute(
          ( "traverse-successors %s %s " +
          " &&! traverse-successors %s %s " ) % (id, depth, without, without_depth),
          source, sink, row_munger = row_munger)
         

    @staticmethod
    def isValidCommandName(name):   #static #OK
        """Check if the given name is a valid command name.

        Command names consist of a letter followed by any number of letters,
        numbers, or dashes.
    
        """
        
        if type(name) != str:
            return False
        
        return re.match('^[a-zA-Z_][-\w]*$', name)
         
    
    @staticmethod
    def isValidCommandString(command): #static # fast OK
        """Check if the given string passes some sanity checks.

        The command string must start with a valid command, and it must not
        contain any non-printable or non-ascii characters.
    
        """
        
        if type(command) != str:
            return False
        
        if not re.match('^[a-zA-Z_][-\w]*\s*(:?\s*$|[\s!&]+\w|[|<>#])', command):
            return False        # must start with a valid command
            
        if re.search('[\0-\x1F\x80-\xFF]', command):
            return False        # bad characters

        return True
         
    
    @staticmethod
    def isValidCommandArgument(arg, strict=True): #static #OK
        """ Check if the given string is a valid argument.

        If strict is set, it checks if arg consists of an alphanumeric
        character followed by any number of alphanumerics, colons or dashes.
        If strict is not set, this just checks that arg doesn't contain
        any non-printable or non-ascii characters.
     
        @param string arg the argument to check
        @param bool strict whether to perform a strict check (default: True).
    
        """
        
        if not arg:
            return False
        
        if not type(arg) in (str, unicode, int, long):
            return False
        
        if strict:
            return re.match('^\w[-\w]*$', str(arg))
        else:
            return not re.search('[\s\0-\x1F\x80-\xFF|<>!&#]', str(arg))
            # low chars, high chars, and operators.
    
    @staticmethod
    def splitRow(s):
        """Convert a line from a data set into a tuple.
    
        If s is empty, this method returns False. if s starts with "#", it's
        considered to consist of a single string field. Otherwise, the
        string is split on ocurrances of TAB, semikolon or comma. Numeric
        field values are converted to int, other fields remain strings.
         
        @param string s the row from the data set, as a string
        @return array containing the fields in s, or false if s is empty.
        
        """
        if not s:
            return False
        if s[0] == '#':
            row = (s[1],) #full line string text
        else:
            row = re.split(' *[;,\t] *', s)
            
            for i, entry in enumerate(row):
                if re.match('^\d{1,9}$', entry): #TODO: port to python: no more than 9 chars for int conversion!
                    row[i] = int(entry)
                    
            row = tuple(row)
            
        return row
    
    @staticmethod
    def joinRow(row): # fertig.
        """Create a string representing the data set `row'.
        
        joinRow tries to convert `row' to a reasonable string
        representation. Numbers can be passed either as int or as str
        types. If a string is passed or the list/tuple has only one
        string which represents no number, this string will be marked
        with a leading `#' and then be returned.
        
        If `row' is a tulple/list containing str or int types, those
        will be returned as comma seperated values.
        
        @type row:  str, or list/tuple of int/str types
        @param row: The data row
        @rtype:  str
        @return: string containing the fields from row
        
        """
        #if not row:
        #    return '' #? This case is covered by join(...).
        if isinstance(row, str):
            return '#' + row
        if len(row) == 1 and isinstance(row[0], str) and \
          not re.match('^\d+$', row[0]):
            return '#' + row[0]
        try:
            s = ','.join("%s" % el for el in row)
        except:
            #print row
            raise
        return s
    
    def _copyFromSource(self, source, row_munger = None):
        """Pass data from source to client line by line.
    
        Copies all data from the given source into the current command stream,
        that is, passes them to the client line by line. 
         
        Note that this must only be called after passing a command line
        terminated by ":" to the peer, so the peer expects a data set.
        
        This is implemented by calling the transport's make_sink() method
        to create a sink for writing to the command stream, and then using
        the copy() method to transfer the data.
        
        Note that source is not automatically closed by this method.
        
        """
        sink = self.transport.make_sink()
        self._trace(__function__(), "source", type(source))
        self.copy(source, sink, ' > ', row_munger = row_munger)
        # source.close()        # to close or not to close...
        self.transport.send( LINEBREAK )     #XXX: flush again??
        self._trace(__function__(), "copy complete.")
        
        # #
        # while ( $row = $source->nextRow() )  
            # $s = Connection::joinRow( $row );
            # 
            # fputs(self.hout, $s . LINEBREAK);
            #  
        # 
        # fputs(self.hout, LINEBREAK); // blank line
        
         
    
    def _copyToSink(self, sink=None, row_munger = None):
        """Pass data from peer to sink line by line.
    
        Copies all data from the command response into the given sink,
        that is, receives data from the peer line by line. 
        
        Note that this must only be called after the peer sent a response
        line that endes with ":", so we know the peer is waiting to send a
        data set.
        
        This is implemented by calling the transport's make_source() method
        to create a source for reading from the command stream, and then
        using the copy() method to transfer the data.
        
        Note that sink is flushed but not closed before this method returns.
        
        """
        source = self.transport.make_source()
        self._trace(__function__(), "sink", type(sink))
        self.copy(source, sink, ' < ', row_munger = row_munger )
        self._trace(__function__(), "copy complete.")
        # $source->close();     # to close or not to close...
        
        # #
        # while ( $s = fgets(self.hin) )  
            # $s = trim($s);
            # if ( $s === '' ) break;
            # 
            # $row = Connection::splitRow( $s );
            # 
            # if ( $sink )  
                # $sink->putRow( $row );
                #  
        
    def copy(self, source, sink=None, indicator = '<>', row_munger = None): #OK
        """Transfer all rows from a data source to a data sink.

        Utility method. If sink is None, all rows are read from the
        source and then discarded.
        Before returning, the sink is flushed to commit any pending data.
    
        @type source:  DataSource
        @param source: source of the data rows
        @type sink:  DataSink
        @param sink: sink where the rows are transferred to.
        @type indicator:  str
        @param indicator: the message prefix to show in debug-mode
        @param row_munger: a callback function to be invoked for every row copied.
               the return value from the munger replaces the original row.
               If the munger function returns None or False, the row is skipped.
        
        """
        for row in source:
            if row_munger: #TODO: PORT TO PHP
                row = row_munger(row)
                
                if not row:
                    continue
                
            if sink:
                self._trace(__name__, indicator, row)
                sink.putRow(row)
            else:
                self._trace(Connection.copy, "#", row)
                
        if ( sink ):
            sink.flush()
    
    def close(self): #OK
        """Closes this connection by closing the underlying transport."""
        self.transport.close()

    @staticmethod
    def new_client_connection(graphname, host=False, port=False): # static #OK
        """Return a new ClientTransport connection.
        
        Create a new connection for accessing a remote graph
        managed by a GraphServ service. Returns a Connection
        that uses a ClientTransport to talk to the remote graph.
     
        @param string graphname the name of the graph to connect to
        @param string host (default: 'localhost') the host the
        GraphServ process is located on.
        @param int port (default: PORT) the TCP port the
        GraphServ process is listening on.
    
        """
        conn = Connection( ClientTransport(host, port), graphname ) #FIXME: PORT graphname stuff to PHP!
        
        return conn
    
    @staticmethod
    def new_slave_connection(command, cwd=None, env=None): #static #OK
        """Return a new SlaveTransport connection.
        
        Create a new connection for accessing a graph managed by a 
        slave GraphCore process. Returns a Connection that uses a 
        SlaveTransport to talk to the local graph.
     
        @param mixed command the command line to start GraphCore.
               May be given as a string or as an array.
               If given as a string, all parameters must be duely
               escaped #?. If given as an array, command[0] must be
               the path to the GraphCore executable.
               See Slavetransport.makeCommand() for more details.
        @param string cwd (default: None) the working dir to run
               the slave process in. Defaults to the current working directory.
        @param int env (default: None) the environment variables
               to pass to the slave process. Defaults to inheriting
               the PHP script's environment.
    
        """
        return Connection(SlaveTransport(command, cwd, env))

def array_column(a, col):
    """Extracts a column from a tabular structure
 
    @type a:  list/tuple
    @param a: an array of equal-sized arrays, representing
              a table as a list of rows.
    @type col:  usually int or str
    @param col: the column key of the column to extract

    @rtype:  list
    @return: the values of column col from each row in a

    """
    column = []
    
    for r in a:
        column.append( r[col] )
    
    return column

def pairs2map( pairs, key_col=0, value_col=1):
    """Converts a list of key value pairs to a dictionary.

    @type pairs:  array
    @param pairs: an array of key value paris,
                  representing a map as a list of tuples.
    @type key_col:  mixed
    @param key_col: the column that contains the key (default: 0)
    @type value_col:  mixed
    @param value_col: the column that contains the value (default: 1)

    @rtype:  dictionary
    @return: the key value pairs in pairs.

    """
    
    m = {}
    for p in pairs:
        k = p[key_col]
        m[ k ] = p[value_col]
        
    return m
     
def escapeshellcmd(command):
    return '"%s"' % (
        command
        .replace('#', '\#')
        .replace('&', '\&')
        .replace(';', '\.')
        .replace('`', '\`')
        .replace('|', '\|')
        .replace('*', '\*')
        .replace('~', '\~')
        .replace('<', '\<')
        .replace('>', '\>')
        .replace('^', '\^')
        .replace('(', '\(')
        .replace(')', '\)')
        .replace('[', '\[')
        .replace(']', '\]')
        .replace('{', '\{')
        .replace('}', '\}')
        .replace('$', '\$')
        .replace(',', '\,')
        .replace('\'', '\\\'')
        .replace('\"', '\\"')
    )
def escapeshellarg(arg):
   return '\'' + arg.replace('\'', '\'' + '\\' + '\'' + '\'') + '\''
