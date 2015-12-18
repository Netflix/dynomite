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
import string

from logging import debug, info, warning, error

import redis

num_conn = 5
dot_rate = 10
current_milli_time = lambda: int(round(time.time() * 1000))
threadLock = threading.Lock()
threads = []
conns = []
payload_prefix = 'value_'


class OperationThread (threading.Thread):
    def __init__(self, threadID, name, options, start_num, end_num):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.options = options
        self.start_num = start_num
        self.end_num = end_num


    def run(self):
        operation = self.options.operation
        host = self.options.host
        port = self.options.port
        start = self.start_num
        end   = self.end_num
        print "Starting thread: " + self.name +  ", start: " + str(start) + " and end: " + str(end)

        # Get lock to synchronize threads
        #threadLock.acquire()
    
        if 'write' == operation :
           write_ops(start, end, host, port, db=0)

        elif 'read' == operation :
           read_ops(start, end, host, port, db=0)

        elif 'mread' == operation :
           mread_ops(start, end, host, port, db=0)

        elif 'del' == operation :
           del_ops(start, end, host, port, db=0)

        elif 'swrite' == operation :
           r = redis.StrictRedis(host, port, db=0)
           r.set('key_time', str(current_milli_time()))
        elif 'sread' == operation :
           r = redis.StrictRedis(host, port, db=0)
           is_stop = False

           while not is_stop:
              value = r.get('key_time')
              if value != None :
                 is_stop = True

           print 'Estimated elapsed time : ' + str(current_milli_time() - int(value))
        elif 'sdel' == operation :
           r = redis.StrictRedis(host, port, db=0)
           r.delete('key_time')
        elif 'flushall' == operation :
           r = redis.StrictRedis(host, port, db=0)
           r.flushall();
    
        # Free lock to release next thread
        #threadLock.release()




def get_conns(host, port, db, num):
    for i in range(0, num):
       conns.append(redis.StrictRedis(host, port, db=0))
    return conns

def generate_value(i):
    return payload_prefix + '_' + str(i)

def write_ops(skipkeys, numkeys, host, port, db):
    conns = get_conns(host, port, db, num_conn)
    start = int(skipkeys)
    end   = int(numkeys)
    print 'start: ' + str(start) + ' and end: ' + str(end)
    print 'payload_prefix: ' + payload_prefix

    for i in range(start, end ) :
        r = conns[i % num_conn]
        if (i % dot_rate == 0) :
           sys.stdout.write('.')
        try:
           r.set('key_' + str(i), generate_value(i))
        except redis.exceptions.ResponseError:
           print "reconnecting ..."
           r = redis.StrictRedis(host, port, db=0)
           conns[i % num_conn] = r


def read_ops(skipkeys, numkeys, host, port, db):
    #r = redis.StrictRedis(host, port, db=0)
    conns = get_conns(host, port, db, num_conn)
    start = int(skipkeys)
    end   = int(numkeys)
     
    print 'start: ' + str(start) + ' and end: ' + str(end)
    error_count = 0
    for i in range(start, end ) :
        r = conns[i % num_conn]
        try:
            value = r.get('key_' + str(i))
        except redis.exceptions.ResponseError:
            print "reconnecting ..."
            r = redis.StrictRedis(host, port, db=0)

        if value is None:
            error_count = error_count + 1
            print 'No value for key: ' + 'key_' + str(i)
        elif value != generate_value(i):
            print 'key_' + str(i) + ' has incorrect value '
            error_count += 1

    print 'Error count: ' + str(error_count) 


def del_ops(skipkeys, numkeys, host, port, db):
    #r = redis.StrictRedis(host, port, db=0)
    conns = get_conns(host, port, db, num_conn)
    start = int(skipkeys)
    end   = int(numkeys)

    print 'start: ' + str(start) + ' and end: ' + str(end)   

    for i in range(start, end ) :
       r = conns[i % num_conn]
       if (i % dot_rate == 0) :
           sys.stdout.write('.')

       try:
           r.delete('key_' + str(i))
       except redis.exceptions.ResponseError:
           print "reconnecting ..."
           r = redis.StrictRedis(host, port, db=0) 


def mread_ops(skipkeys, numkeys, host, port, db):
       r = redis.StrictRedis(host, port, db=0)
       start = int(skipkeys)
       end   = int(numkeys)

       print 'start: ' + str(start) + ' and end: ' + str(end)   

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

       while (len(keys) > 0) :
          values = r.mget(keys)
          print values
          for key in values.keys() :
              keys.remove(key)



def main():
    parser = OptionParser(usage="usage: %prog [options] filename",
                          version="%prog 1.0")
    parser.add_option("-t", "--threads",
                      action="store",
                      dest="th",
                      default="1",
                      help="Number of client threads. Default is 1")
    parser.add_option("-o", "--operation",
                      action="store",
                      dest="operation",
                      default="write",
                      help="Operation to perform: write, read, del, swrite (single write), sread (polling single read), and sdel")
    parser.add_option("-l", "--logfile",
                      action="store",
                      dest="logfle",
                      default="/tmp/dynomite-test.log",
                      help="log file location. Default is /tmp/dynomite-test.log")
    parser.add_option("-H", "--host",
                      action="store",
                      dest="host",
                      default="127.0.0.1",
                      help="targe host ip. Default is 127.0.0.1")
    parser.add_option("-P", "--port",
                      action="store",
                      dest="port",
                      default="8102",
                      help="target port. Default is 8102")
    parser.add_option("-S", "--skipkeys",
                      action="store",
                      dest="skipkeys",
                      default="0",
                      help="target port. Default is 0")
    parser.add_option("-n", "--numkeys",
                      action="store",
                      dest="numkeys",
                      default="100",
                      help="Number of keys. Default is 100\n")
   
    parser.add_option("-s", "--payloadsize",
                      action="store",
                      dest="payloadsize",
                      default="6",
                      help="Size of payload in bytes. Default is 6 bytes\n")


    if len(sys.argv) == 1:
         print "Learn some usages: " + sys.argv[0] + " -h"
         sys.exit(1)


    (options, args) = parser.parse_args()

    print options
    start = int(options.skipkeys)
    end   = int(options.numkeys)
    global payload_prefix
    payload_prefix = ''.join('a' for i in range(int(options.payloadsize)))

    num_threads = int(options.th)

    step = (end - start) / num_threads

    print "step " + str(step)

    for i in range(0, num_threads):
       if (i != num_threads-1):
          thread = OperationThread(i, "Thread-" + str(i), options, start + (i*step), start + (i+1)*step)
       else:
          thread = OperationThread(i, "Thread-" + str(i), options, start + (i*step), end)
       #thread = OperationThread(1, "Thread-1", options, 1, 1000)

       thread.start()
       threads.append(thread)

    for t in threads:
       t.join()

    print ""


if  __name__ == '__main__':
    main()
