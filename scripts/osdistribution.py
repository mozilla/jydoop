# mapreduce job to count the distribution of OS data

import json
import random

def map(k, v, cx):
    if random.random() > 0.05:
        return
    j = json.loads(v)
    os = j['info']['OS']
    cx.write(os, 1)

def reduce(k, vlist, cx):
    cx.write(k, sum(vlist))
