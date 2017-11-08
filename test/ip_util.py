import socket
import struct

def quad2int(ip):
    return struct.unpack("!L", socket.inet_aton(ip))[0]

def int2quad(ip):
    return socket.inet_ntoa(struct.pack('!L', ip))
