#! /usr/bin/env python3
"""Run Souffle Datalog programs tests in batches"""

import os
import sys

def exit(code):
    if code != 0:
        print("Terminating with exit code", str(code))
    sys.exit(code)

def main():
    if len(sys.argv) >= 2:
        start = int(sys.argv[1])
    else:
        start = 0

    if len(sys.argv) >= 3:
        end = int(sys.argv[2])
    else:
        end = 0xffffffff

    if len(sys.argv) >= 4:
        batchsize = int(sys.argv[3])
    else:
        batchsize = 5

    while start <= end:
        print("Running from", start)
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
