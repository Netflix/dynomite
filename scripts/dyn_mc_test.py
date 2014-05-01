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
import random

from logging import debug, info, warning, error


import memcache


current_milli_time = lambda: int(round(time.time() * 1000))


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
                      help="Operation to perform: write, read, del, swrite (single write), sread (polling single read), and sdel")
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
    parser.add_option("-S", "--skipkeys",
                      action="store",
                      dest="skipkeys",
                      default="0",
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

    #should do some try/catch but I am lazy now

    mc = memcache.Client([options.host + ':' + options.port], debug=0)
    numkeys = int(options.numkeys)
    start = int(options.skipkeys)
    end   = int(options.numkeys)
    print 'start: ' + str(start) + ' and end: ' + str(end)

    if 'write' == options.operation :
       for i in range(start, end ) :
           mc.set('key_' + str(i), 'value_' + str(i))

    elif 'read' == options.operation :
       error_count = 0
       for i in range(start, end ) :
          value = mc.get('key_' + str(i))
          if value is None:
             error_count = error_count + 1
             print 'No value for key: ' + 'key_' + str(i)
          else :
             print 'key_' + str(i) + ' has value : ' + value
       print 'Errour count: ' + str(error_count)
    elif 'mread' == options.operation :
       n = (end - start) / 10
       n = min(n, 10)
       print n
       keys = []
       i = 0
       while (i < n) :
           ran = random.randint(start, end-1)
           key = 'key_' + str(ran)
           if key not in keys :
              keys.append(key)
              i = i + 1
       print keys

      #values = mc.get_multi(['key_1', 'key_2', 'key_3'])
       while (len(keys) > 0) :
         values = mc.get_multi(keys)
         print values
         for key in values.keys() :
             keys.remove(key)



    elif 'del' == options.operation :
         for i in range(start, end ) :
             mc.delete('key_' + str(i))
    elif 'swrite' == options.operation :
         mc.set('key_time', str(current_milli_time()))
    elif 'sread' == options.operation :
         is_stop = False

         while not is_stop:
           value = mc.get('key_time')
           if value != None :
               is_stop = True

         print 'Estimated elapsed time : ' + str(current_milli_time() - int(value))

    elif 'sdel' == options.operation :
        mc.delete('key_time')


    mc.disconnect_all()


if __name__ == '__main__':
    main()

