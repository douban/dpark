import math

class Vector:

    def __init__(self, l):
        self.data = l

    def __add__(self, o):
        return Vector([a+b for a,b in zip(self.data, o.data)])

    def __sub__(self, o):
        return Vector([a-b for a,b in zip(self.data, o.data)])

    def __div__(self, n):
        return Vector([i/n for i in self.data])

    def __repr__(self):
        return '[%s]' % (','.join('%.1f'% i for i in self.data))

    def dot(self, o):
        return sum(a*b for a,b in zip(self.data, o.data))

    def squaredDist(self, o):
        return sum((a-b)*(a-b) for a,b in zip(self.data, o.data))
        
    def sum(self):
        return sum(self.data)

    def dist(self, o):
        return math.sqrt(self.squaredDist(o))
