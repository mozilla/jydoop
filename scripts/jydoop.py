"""
Standard library of useful things for jydoop scripts.
"""

def isJython():
    import platform
    return platform.system() == 'Java'
        
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
    if isJython():
        import org.mozilla.jydoop.PythonValue as PythonValue
        import org.python.core.util.FileUtil as FileUtil
        f = FileUtil.wrap(PythonValue().getClass().getClassLoader().getResourceAsStream(path))
    else:
        # Python case
        f = open(path, 'r')
    return f.read()
