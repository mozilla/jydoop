"""
Standard library of useful things for jydoop scripts.
"""

def sumreducer(k, vlist, cx):
    """
    Simple function which can be used as a combiner and reducer to compute
    a sum total for a particular key.
    """
    cx.write(k, sum(vlist))

"""
Read something out of driver.jar
"""
def getResource(path):
    try:
        # Jython case
        import org.mozilla.jydoop.TypeWritable as TypeWritable
        import org.python.core.util.FileUtil as FileUtil
        f = FileUtil.wrap(TypeWritable().getClass().getClassLoader().getResourceAsStream(path))
    except ImportError:
        # Python case
        f = open(path, 'r')
    return f.read()
