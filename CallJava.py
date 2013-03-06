def callme(out):
    out.println("from python")

def map(value, one, outword, context):
    for f in value.toString().split(" "):
        outword.set(f)
        context.write(outword, one);
        
