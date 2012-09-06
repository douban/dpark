# utils 

def ilen(x):
    try:
        return len(x)
    except TypeError:
        return sum(1 for i in x)
