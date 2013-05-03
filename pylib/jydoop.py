"""
Standard library of useful things for jydoop scripts.
"""

import csv

def isJython():
    import platform
    return platform.system() == 'Java'

def sumreducer(k, vlist, cx):
    """
    Simple function which can be used as a combiner and reducer to compute
    a sum total for a particular key.
    """
    cx.write(k, sum(vlist))

def getResource(path):
    """
    Read something out of driver.jar
    """
    if isJython():
        import org.mozilla.jydoop.PythonValue as PythonValue
        import org.python.core.util.FileUtil as FileUtil
        f = FileUtil.wrap(PythonValue().getClass().getClassLoader().getResourceAsStream(path))
    else:
        # Python case
        f = open(path, 'r')
    return f.read()

def unwrap(l, v):
    """
    Unwrap a value into a list. Dicts are added in their repr form.
    """
    if isinstance(v, (tuple, list)):
        for e in v:
            unwrap(l, e)
    elif isinstance(v, dict):
        l.append(repr(v))
    else:
        l.append(v)

def outputWithKey(path, results):
    """
    Output key/values into a reasonable CSV.

    All lists/tuples are unwrapped.
    """

    f = open(path, 'w')
    w = csv.writer(f)
    for k, v in results:
        l = []
        unwrap(l, k)
        unwrap(l, v)
        w.writerow(l)

def outputWithoutKey(path, results):
    """
    Output values into a reasonable text file. If the values are simple,
    they are printed directly. If they are complex tuples/lists, they are
    printed as csv.
    """
    f = open(path, 'w')
    w = csv.writer(f)
    for k, v in results:
        l = []
        unwrap(l, v)
        if len(l) == 1:
            print >>f, v
        else:
            w.writerow(l)
