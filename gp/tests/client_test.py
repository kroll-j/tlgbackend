#!/usr/bin/python
# -*- coding: utf-8

import unittest
import os
import tempfile
from test_base import *
from gp.client import *

test_graph_name = 'test' + str(os.getpid())
TestFilePrefix = '/tmp/gptest-' + str(os.getpid())

class ClientTest (ClientTestBase, unittest.TestCase):
    """Test the TCP client connection.

    Client Connection Tests
    currently none. Could test handling of TCP issues, etc

    @TODO: (optionally) start server instance here!
          let it die when the test script dies.
          
    @TODO: CLI interface behaviour of server (port config, etc)

    """


if __name__ == '__main__':
	unittest.main()
