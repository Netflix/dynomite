#!/usr/bin/env python3

class Node(object):
    def __init__(self, ip, port):
        self.ip = ip
        self.port=port
        self.name="Node: %s:%d" % (ip, port)

    def __name__(self):
        return self.name

    def start(self):
        return

    def stop(self):
        return
