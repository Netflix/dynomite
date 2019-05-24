#!/usr/bin/env python3
import redis

class ResultMismatchError(Exception):
    def __init__(self, r_result, d_result, func, *args):
        self.r_result = r_result
        self.d_result = d_result
        self.func = func
        self.args = args
    def __str__(self):
        ret = "\n\t======Result Mismatch=======\n"
        ret += "\tQuery: %s %s" % (self.func, str(self.args))
        ret += "\n\t===========================\n"
        ret += "\tRedis: %s" % str(self.r_result)
        ret += "\n\t===========================\n"
        ret += "\tDyno: %s" % str(self.d_result)
        return ret

class dual_run():
    def __init__(self, r, d, debug=None):
        self.r = r
        self.d = d
        self.debug = debug
        self.sort_before_cmp = False

    # If 'self.sort_before_cmp' is True, we sort the return values (if they're of the
    # list type) from Dynomite and Redis before comparing them so that we have ordered
    # comparison.
    def set_sort_before_compare(self, should_sort):
        self.sort_before_cmp = should_sort

    def run_verify(self, func, *args):
        r_result = None
        d_result = None
        r_func = getattr(self.r, func)
        d_func = getattr(self.d, func)
        r_result = r_func(*args)
        i = 0
        retry_limit = 3
        while i < retry_limit:
            try:
                d_result = d_func(*args)
                if i > 0:
                    print("\tSucceeded in attempt {}".format(i+1))
                break
            except redis.exceptions.ResponseError as e:
                if "Peer Node is not connected" in str(e):
                    i = i + 1
                    print("\tGot error '{}' ... Retry effort {}/{}\n\tQuery '{} {}'".format(e, i, retry_limit, func, str(args)))
                    continue
                print("\tGot error '{}'\n\tQuery '{} {}'".format(e, func, str(args)))
                break
        if self.debug:
            print("Query: %s %s" % (func, str(args)))
            print("Redis result: %s" % str(r_result))
            print("Dyno result: %s" % str(d_result))

        if (self.sort_before_cmp and isinstance(r_result, list)):
            r_result.sort()
            d_result.sort()

        if r_result != d_result:
            raise ResultMismatchError(r_result, d_result, func, *args)
        return d_result

    def run_dynomite_only(self, func, *args):
        d_result = None
        d_func = getattr(self.d, func)
        i = 0
        retry_limit = 3
        while i < retry_limit:
            try:
                d_result = d_func(*args)
                if i > 0:
                    print("\tSucceeded in attempt {}".format(i+1))
                break
            except redis.exceptions.ResponseError as e:
                if "Peer Node is not connected" in str(e):
                    i = i + 1
                    print("\tGot error '{}' ... Retry effort {}/{}\n\tQuery '{} {}'".format(e, i, retry_limit, func, str(args)))
                    continue
                print("\tGot error '{}'\n\tQuery '{} {}'".format(e, func, str(args)))
                break
        if self.debug:
            print("Query: %s %s" % (func, str(args)))
            print("Dyno result: %s" % str(d_result))
        return d_result
