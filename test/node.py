#!/usr/bin/env python3

class Node(object):
    def __init__(self, host="localhost", ip="127.0.0.1", port=1212):
        self.host=host
        self.port=port
        self.name="Node: %s:%d" % (host, port)

    def __name__(self):
        return self.name

    def start(self):
        return

    def stop(self):
        return
