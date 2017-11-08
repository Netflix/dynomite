#!/usr/bin/env python3

from optparse import OptionParser
import configparser
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


class OperationThread (threading.Thread):
    def __init__(self, threadID, name, options):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.options = options
        self.filename = options.filename


    def run(self):
        operation = self.options.operation
        host = self.options.host
        port = self.options.port

        print("Starting thread: " + self.name +  ", filename: " + self.filename)

        # Get lock to synchronize threads
        #threadLock.acquire()

        if 'rebalance' == operation :
           rebalance_ops(self.filename, host, port, db=0)


        # Free lock to release next thread
        #threadLock.release()



def get_conns(host, port, db, num):
    for i in range(0, num):
       conns.append(redis.StrictRedis(host, port, db=0))
    return conns

def generate_value(i):
    return payload_prefix + '_' + str(i)


def rebalance_ops(filename, host, port, db):
    conns = get_conns(host, port, db, 2)
    r1 = conns[0]
    i = 0
    for line in open(filename,'r').readlines():
        if line != '':
          print(line)
          line = line.strip('\n')
          if line == '':
            continue
          try:

            value = r1.delete(line)
            #r2.set(line, value)
            i = i + 1
            if (i % 5000 == 0):
              time.sleep(1)
          except redis.exceptions.ResponseError:
            print("reconnecting ...")
            r1 = redis.StrictRedis(host, port, db=0)



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
                      default="rebalance",
                      help="Operation to perform: rebalance")
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
    parser.add_option("-f", "--filename",
                      action="store",
                      dest="filename",
                      default="0",
                      help="target port. Default is 0")



    if len(sys.argv) == 1:
         print("Learn some usages: " + sys.argv[0] + " -h")
         sys.exit(1)


    (options, args) = parser.parse_args()

    print(options)

    num_threads = int(options.th)


    for i in range(0, num_threads):
       if (i != num_threads-1):
          thread = OperationThread(i, "Thread-" + str(i), options)
       else:
          thread = OperationThread(i, "Thread-" + str(i), options)
       #thread = OperationThread(1, "Thread-1", options, 1, 1000)

       thread.start()
       threads.append(thread)

    for t in threads:
       t.join()

    print()


if  __name__ == '__main__':
    main()

