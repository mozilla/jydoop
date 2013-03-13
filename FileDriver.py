class LocalContext:
    def __init__(self):
        self.result = {}

    def write(self, key, value):
        try:
            self.result[key].append(value)
        except KeyError:
            self.result[key] = [value]

    def dump(self):
        for key, values in self.result.iteritems():
            for v in values:
                print "%s %s" % (key, v)

def map_reduce(module, file):
    context = LocalContext()
    f = open(file)
    for l in f.readlines():
        module.map(str(f.tell()), l, context)
    f.close()
    try:
        reducefunc = module.reduce
    except AttributeError:
        reducefunc = None
        context.dump()
        return
    reduced_context = LocalContext()
    for key, values in context.result.iteritems():
        module.reduce(key, values, reduced_context)
    reduced_context.dump()
    
