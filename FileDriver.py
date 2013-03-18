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

def map_reduce(module, file):
    try:
        reducefunc = module.reduce
    except AttributeError:
        reducefunc = None

    context = LocalContext(reducefunc == None)

    if file == "-":
        f = sys.stdin
    else:
        f = open(file)
    total = 0;
    while True:
        l = f.readline()
        length = len(l)
        total += length
        if length == 0:
            break
        module.map(str(total), l, context)
    f.close()

    reduced_context = LocalContext()
    for key, values in context.result.iteritems():
        module.reduce(key, values, reduced_context)
    reduced_context.dump()
    
