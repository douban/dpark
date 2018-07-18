# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
from dpark import DparkContext
from dpark.mutable_dict import MutableDict
from random import shuffle
import six.moves.cPickle
import numpy
from six.moves import range
from six.moves import zip

dpark = DparkContext()

with open('ab.mat') as f:
    ori = six.moves.cPickle.loads(f.read())

k = 50
d = 20
M = len(ori)
V = len(ori[0])
assert M % d == 0
assert V % d == 0

m = M / d
v = V / d

GAMMA = 0.02
LAMBDA = 0.1
STEP = 0.9

W = MutableDict(d)
H = MutableDict(d)

ori_b = dpark.broadcast(ori)


def sgd(i_j):
    (i, j) = i_j
    Wi = W.get(i)
    if Wi is None:
        Wi = numpy.random.rand(m, k)
        W.put(i, Wi)

    Hj = H.get(j)
    if Hj is None:
        Hj = numpy.random.rand(v, k)
        H.put(j, Hj)

    ori = ori_b.value
    Oij = ori[i * m:(i + 1) * m, j * v:(j + 1) * v]

    for x in range(m):
        for y in range(v):
            pred = Wi[x].dot(Hj[y])
            err = int(Oij[x][y]) - int(pred)
            w = Wi[x] + GAMMA * (Hj[y] * err - LAMBDA * Wi[x])
            h = Hj[y] + GAMMA * (Wi[x] * err - LAMBDA * Hj[y])

            Wi[x] = w
            Hj[y] = h

    W.put(i, Wi)
    H.put(j, Hj)


rdd = dpark.makeRDD(list(range(d)))
rdd = rdd.cartesian(rdd).cache()


def calc_err(i_j):
    (i, j) = i_j
    Wi = W.get(i)
    Hj = H.get(j)

    ori = ori_b.value
    Rij = Wi.dot(Hj.T)
    Oij = ori[i * m:(i + 1) * m, j * v:(j + 1) * v]
    return ((Rij - Oij) ** 2).sum()


J = list(range(d))
while True:
    for i in range(d):
        dpark.makeRDD(list(zip(list(range(d)), J)), d).foreach(sgd)
        J = J[1:] + [J[0]]

    GAMMA *= STEP
    shuffle(J)
    err = rdd.map(calc_err).reduce(lambda x, y: x + y)
    rmse = numpy.sqrt(err / (M * V))
    print(rmse)
    if rmse < 0.01:
        break
