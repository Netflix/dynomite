#!/usr/bin/env python

from optparse import OptionParser
import ConfigParser
import logging
import time
import os
import re
import sys
import errno
from datetime import datetime
from datetime import timedelta
import threading


from logging import debug, info, warning, error


import memcache




def main():
    parser = OptionParser(usage="usage: %prog [options] filename",
                          version="%prog 1.0")
    parser.add_option("-t", "--threads",
                      action="store",
                      dest="th",
                      default="1",
                      help="Number of client threads")
    parser.add_option("-o", "--operation",
                      action="store",
                      dest="operation",
                      default="write",
                      help="Operation to perform")
    parser.add_option("-l", "--logfile",
                      action="store",
                      dest="logfle",
                      default="/tmp/dynomite-test.log",
                      help="log file location")
    parser.add_option("-H", "--host",
                      action="store",
                      dest="host",
                      default="127.0.0.1",
                      help="targe host ip")
    parser.add_option("-P", "--port",
                      action="store",
                      dest="port",
                      default="8102",
                      help="target port")
    parser.add_option("-n", "--numkeys",
                      action="store",
                      dest="numkeys",
                      default="100",
                      help="Number of keys\n")


    if len(sys.argv) == 1:
         print "Learn some usages: " + sys.argv[0] + " -h"
         sys.exit(1)


    (options, args) = parser.parse_args()



    #logger = logging.getLogger(log_name)
    #logger.setLevel(logging.DEBUG)
    #fh = logging.handlers.TimedRotatingFileHandler('/tmp/dynomite-test.log', when="midnight")
    #fh.setLevel(logging.DEBUG)
    #formatter = logging.Formatter('%(asctime)s:  %(name)s:  %(levelname)s: %(message)s')
    #fh.setFormatter(formatter)
    #logger.addHandler(fh)


    print options

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s',
                        filename='/tmp/dynomite-test.log',
                        filemode='w')

    mc = memcache.Client([options.host + ':' + options.port], debug=0)
    numkeys = int(options.numkeys)

    if 'write' == options.operation :
       for i in range(1, numkeys ) :
           mc.set('key_' + str(i), 'value_' + str(i))


    elif 'read' == options.operation :
       for i in range(1, numkeys ) :
          value = mc.get('key_' + str(i))
          if value is None:
             print 'No value for key: ' + 'key_' + str(i)
          print 'key_' + str(i) + ' has value : ' + value





if __name__ == '__main__':
    main()


