# mapreduce job to count the distribution of OS data

import json

def map(k, v, cx):
    j = json.loads(v)
    os = j['info']['OS']
    cx.write(os, 1)

def reduce(k, vlist, cx):
    cx.write(k, sum(vlist))
