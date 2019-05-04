#! /usr/bin/env python2.7
"""Run various Souffle Datalog programs tests"""

import os
import subprocess
import sys
import shutil

tests_run = 0
tests_passed = 0

# expected to fail
xfail = [
    "souffle7",  # recursive type
    "souffle8",  # component inheritance
    "souffle9",  # generic component
    "souffle10", # generic component
    "access2",   # input relation modified
    "access3",   # same
    "aggregates2", # max in relation argument
    "aliases",   # assignments to tuples containing variables
    "comp-override1", # component inheritance
    "comp-override2", # same
    "comp-override3", # same
    "components",     # same
    "components3",    # same
    "components1",    # component instantiation
    "components_generic", # generic component
    "functor_arity",  # min, max, cat with more than 2 arguments
    "grammar",        # funny unicode char in a comment
    "independent_body2", # not in DNF form
    "inline_nqueens", # recursive type
    "magic_2sat",     # issue 197
    "magic_centroids", # input relation modified
    "magic_components", # component inheritance
    "magic_dfa",        # same
    "magic_dominance",  # same
    "magic_nqueens",    # recursive type
    "magic_strategies", # component inheritance
    "magic_turing1",    # issue 198
    "math",             # Trigonometric functions and FP types
]

def run_command(command):
    """Runs a command in a shell; returns the stdout; on error prints stderr"""
    print "Running", command
    p = subprocess.Popen(command,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0 and stderr is not None:
        sys.stderr.write("{}".format(stderr))
    return p.returncode, "{}".format(stdout)

def compile_example(directory, f):
    """Run the Souffle example in directory 'directory'"""
    print "Testing " + directory
    global tests_run, tests_passed
    tests_run = tests_run + 1
    os.chdir(directory)
    code, _ = run_command(["../../tools/souffle-converter.py", f, "souffle"])
    if code == 0:
        code, _ = run_command(["ddlog", "-i", "souffle.dl", "-L", "../../lib"])
    if code == 0:
        tests_passed = tests_passed + 1
    else:
        exit(1)
    os.chdir("..")
    return code

def run_examples():
    """Run the hand-created examples; these are all named like 'souffle*'"""
    directories = os.listdir(".")
    for d in directories:
        if os.path.isfile(d):
            continue
        if "souffle" not in d:
            continue
        if d in xfail:
            print "Expected to fail", d
            continue
        compile_example(d, "test.dl")

def run_remote_tests():
    testgroups = ["evaluation", "example"]
    url = "https://github.com/souffle-lang/souffle/trunk/tests/"
    for tg in testgroups:
        code, dirs = run_command(["svn", "ls", url + tg])
        if code != 0:
            return
        for directory in dirs.split("/\n"):
            if directory in xfail:
                print "Expected to fail", directory
                continue
            if directory[0] < 'a':
                continue
            if not os.path.isdir(directory):
                code, _ = run_command(["svn", "export", url + tg + "/" + directory])
                if code != 0:
                    continue
                code = compile_example(directory, directory + ".dl")
            if code == 0:
                shutil.rmtree(directory)

def main():
    run_examples()
#    run_remote_tests()
    print "Ran", tests_run, "out of which", tests_passed, "passed"

if __name__ == "__main__":
    main()
