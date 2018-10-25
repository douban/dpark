#!/usr/bin/env python

import os.path
import time
from collections import Counter
from dpark.file_manager import open_file, CHUNKSIZE, walk
from glob import glob


def get_sizes(f):
    num_block = (f.length + CHUNKSIZE)/CHUNKSIZE
    d = Counter()
    for i in range(num_block):
        c = f.get_chunk(i)
        d[c.id] = c.length * len(c.addrs)
    return d


def file_length(f):
    n = 0
    for line in f:
        n += len(line)
    assert n == f.length
    return n


def walk_dir(path, followlinks=False):
    paths = []
    t = time.time()

    for root, dirs, names in walk(path, followlinks=followlinks):
        for n in sorted(names):
            if not n.startswith('.'):
                p = os.path.join(root, n)
                if followlinks or not os.path.islink(p):
                    paths.append(p)
        dirs.sort()
        for d in dirs[:]:
            if d.startswith('.'):
                dirs.remove(d)
    t = time.time() - t
    print("walk {} files use {}s".format(len(paths), t))
    return paths


def get_files(paths):
    res = []
    for p in paths:
        if os.path.isdir(p):
            res.extend(walk_dir(p))
        else:
            res.append(p)
    return res


def str_size(b):
    ss = ["B", "K", "M", "G", "T"]
    for i in range(len(ss)):
        if (b >> (i * 10)) < 1024:
            return "%.2f%s" % (float(b)/(1 << (i * 10)), ss[i])


def du(paths):
    paths = get_files(paths)
    d1 = Counter()
    d2 = {}
    t = time.time()
    for p in paths:
        f = open_file(p)
        d = get_sizes(f)
        d1 += d
        d2.update(d)
    t = time.time() - t
    fmt = "size {} = {}, real size {} = {}, ratio= {}, use time {}s"
    s1 = sum(d1.values())
    s2 = sum(d2.values())
    print(fmt.format(s1,
                     str_size(s1),
                     s2,
                     str_size(s2),
                     float(s1)/s2,
                     t))


def read_dir(paths):
    paths = get_files(paths)
    t = time.time()
    n = 0
    for p in paths:
        f = open_file(p)
        n += file_length(f)
    t = time.time() - t
    print("total size {}B, use time {}s".format(n, t))
    return n


def file_info(paths):
    paths = get_files(paths)
    for p in paths:
        f = open_file(p)
        print(p)
        num_block = (f.length + CHUNKSIZE)/CHUNKSIZE
        d = Counter()
        for i in range(num_block):
            c = f.get_chunk(i)
            print("\t%03d %d %015d %s" % (i, c.id, c.length, c.addrs))
            d[c.id] = f.length * len(c.addrs)
        return d


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument("op", choices=["du", "read", "walk", "files"])
    parser.add_argument("path",)
    args = parser.parse_args()

    paths = glob(args.path)

    op = args.op
    if op == "walk":
        get_files(paths)
    elif op == "read":
        read_dir(paths)
    elif op == "du":
        du(paths)
    elif op == "files":
        file_info(paths)


if __name__ == "__main__":
    main()
