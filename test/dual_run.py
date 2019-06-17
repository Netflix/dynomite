#!/usr/bin/env python3
import redis

class ResultMismatchError(Exception):
    def __init__(self, r_result, d_result, func, *args, **kwargs):
        self.r_result = r_result
        self.d_result = d_result
        self.func = func
        self.args = args
        self.kwargs = kwargs
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
    def run_verify(self, func, *args, **kwargs):
        r_result = None
        d_result = None
        r_func = getattr(self.r, func)
        d_func = getattr(self.d, func)
        r_result = r_func(*args, **kwargs)
        i = 0
        retry_limit = 3
        while i < retry_limit:
            try:
                d_result = d_func(*args, **kwargs)
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
            print("Query: %s %s" % (func, str(args), str(kwargs)))
            print("Redis result: %s" % str(r_result))
            print("Dyno result: %s" % str(d_result))

        if func in ['xadd']:
            # Truncate milliseconds 
            r_result = str(r_result).split('-')[0][:-5]
            d_result = str(d_result).split('-')[0][:-5]

        if r_result != d_result:
            raise ResultMismatchError(r_result, d_result, func, *args, **kwargs)
        return d_result

    def run_dynomite_only(self, func, *args, **kwargs):
        d_result = None
        d_func = getattr(self.d, func)
        i = 0
        retry_limit = 3
        while i < retry_limit:
            try:
                d_result = d_func(*args, **kwargs)
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
