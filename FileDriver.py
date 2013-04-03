defaultglobals = dict(globals())

class LocalContext:
    def __init__(self, combinefunc=None):
        self.result = {}
        self.combinefunc = combinefunc

    def write(self, key, value):
        self.result.setdefault(key, []).append(value)
        if self.combinefunc and self.result[key].length > 5:
            items = self.result.pop(key)
            self.combinefunc(key, items, self)

    def __iter__(self):
        for k, values in self.result.iteritems():
            for v in values:
                yield k, v

# By default, if the job has a reduce function, we want to print both the key and the value.
# If no reduction is happening, users usually don't care about the key.
def outputwithkey(rlist):
    for k, v in rlist:
        print "%s\t%s" % (k, v)

def outputnokey(rlist):
    for k, v in rlist:
        print v

def map_reduce(module, fd):
    reducefunc = getattr(module, 'reduce', None)
    mapfunc = getattr(module, 'map')

    context = LocalContext()

    # We make fake keys by keeping track of the file offset from the incoming
    # file.
    total = 0;
    for line in fd:
        if len(line) == 0:
            continue
        mapfunc('fake_key_%s' % total, line, context)
        total += len(line)

    if reducefunc:
        reduced_context = LocalContext()
        for key, values in context.result.iteritems():
            module.reduce(key, values, reduced_context)
        context = reduced_context

    if hasattr(module, 'output'):
        outputfunc = module.output
    elif reducefunc:
        outputfunc = outputwithkey
    else:
        outputfunc = outputnokey

    outputfunc(iter(context))
    
if __name__ == '__main__':
    import imp, sys

    if len(sys.argv) != 3:
        print >>sys.stderr, "Usage: FileDriver.py <jobscript.py> <input.data or ->"
        sys.exit(1)

    modulepath, filepath = sys.argv[1:]

    if filepath == "-":
        fd = sys.stdin
    else:
        fd = open(filepath)

    modulefd = open(modulepath)

    module = imp.load_module('pydoop_main', modulefd, modulepath, ('.py', 'U', 1))
    map_reduce(module, fd)
