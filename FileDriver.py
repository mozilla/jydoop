defaultglobals = dict(globals())

import sys

class LocalContext:
    def __init__(self, map_only = False):
        self.result = {}
        self.map_only = map_only

    def write(self, key, value):
        if self.map_only:
            print "%s %s" % (key, value)
            return
        try:
            self.result[key].append(value)
        except KeyError:
            self.result[key] = [value]

    def dump(self):
        for key, values in self.result.iteritems():
            for v in values:
                print "%s %s" % (key, v)

def map_reduce(module, fd):
    reducefunc = getattr(module, 'reduce', None)

    context = LocalContext(reducefunc == None)

    total = 0;
    while True:
        l = fd.readline()
        length = len(l)
        total += length
        if length == 0:
            break
        getattr(module, 'map')(str(total), l, context)

    reduced_context = LocalContext()
    for key, values in context.result.iteritems():
        reducefunc(key, values, reduced_context)
    reduced_context.dump()
    
if __name__ == '__main__':
    import imp

    modulepath, filepath = sys.argv[1:]

    if filepath == "-":
        fd = sys.stdin
    else:
        fd = open(filepath)

    modulefd = open(modulepath)

    module = imp.load_module('pydoop_main', modulefd, modulepath, ('.py', 'U', 1))
    map_reduce(module, fd)
