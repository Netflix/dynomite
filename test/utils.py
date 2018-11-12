#!/usr/bin/env python3
import random
import string

from ip_util import int2quad
from ip_util import quad2int

from itertools import count

BASE_IPADDRESS = quad2int('127.0.1.1')
RING_SIZE = 2**32


def string_generator(size=6, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def number_generator(size=4, chars=string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def dict_request(request):
    """Converts the request into an easy to consume dict format."""

    # We don't ingest the request in this format originally because dict
    # ordering is nondeterministic, and one of our goals is to deterministically
    # generate clusters.
    # TODO: Python 3.6+ has ordered dicts, so the above doesn't hold true from that
    # version.
    return dict(
        (dc['name'], dict(dc['racks']))
        for dc in request
    )

def sum_racks(dcs):
    return dict(
        (name, sum(racks.values()))
        for name, racks in dcs.items()
    )

def pick_tokens(count, start_offset):
    stride = RING_SIZE // count
    token = start_offset
    for i in range(count):
        yield token % RING_SIZE
        token += stride

def tokens_for_rack(count):
    offset = random.randrange(0, RING_SIZE)
    return list(pick_tokens(count, offset))

def tokens_for_dc(racks):
    return [
        (name, tokens_for_rack(count))
        for name, count in racks
    ]

def tokens_for_cluster(dcs, seed):
    if seed is not None:
        random.seed(seed)

    return [
        (dc['name'], tokens_for_dc(dc['racks']))
        for dc in dcs
    ]


def dc_count(dc):
    return sum(count for rack, count in dc)

def generate_ips():
    for ip in count(start=BASE_IPADDRESS, step=1):
        yield int2quad(ip)
