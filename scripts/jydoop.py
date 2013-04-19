"""
Standard library of useful things for jydoop scripts.
"""

def hasjava():
    try:
        import org.python.core.Py
        return True
    except ImportError:
        return False
        
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
    if hasjava():
        import org.mozilla.jydoop.PythonValue as PythonValue
        import org.python.core.util.FileUtil as FileUtil
        f = FileUtil.wrap(PythonValue().getClass().getClassLoader().getResourceAsStream(path))
    else:
        # Python case
        f = open(path, 'r')
    return f.read()
