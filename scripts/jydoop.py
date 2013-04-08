"""
Standard library of useful things for jydoop scripts.
"""

def sumreducer(k, vlist, cx):
    """
    Simple function which can be used as a combiner and reducer to compute
    a sum total for a particular key.
    """
    cx.write(k, sum(vlist))
