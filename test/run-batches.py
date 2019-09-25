#! /usr/bin/env python2.7
"""Run Souffle Datalog programs tests in batches"""

import os
import sys

def exit(code):
    if code != 0:
        print "Terminating with exit code " + str(code)
    sys.exit(code)

def main():
    batchsize = 5
    start = 190

    while True:
        print "Running from", start
        code = os.system("./run-souffle-tests.py -s " + str(start) + " -r " + str(batchsize))
        if code == (2 << 8):
            # run-souffle-tests.py will terminate with exit code 2 when no tests are run
            # Unix will convert an exit code of 2 to 2 << 8
            exit(0)
        elif code != 0:
            exit(code & 0xff | (code >> 8))
        start = start + batchsize

if __name__ == "__main__":
    main()
