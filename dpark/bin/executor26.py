#!/usr/bin/env python2.6
import sys
from os.path import dirname
sys.path.insert(1, dirname(dirname(dirname(__file__))))

from dpark.executor import run

if __name__ == '__main__':
    run()
