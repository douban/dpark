#!/usr/bin/env pypy
import sys
from os.path import dirname
sys.path.insert(1, dirname(dirname(__file__)))

from executor import run

if __name__ == '__main__':
    run()
