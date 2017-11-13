#!/usr/bin/env python3
import random
import string 

def string_generator(size=6, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def number_generator(size=4, chars=string.digits):
    return ''.join(random.choice(chars) for _ in range(size))
