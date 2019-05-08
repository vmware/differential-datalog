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
    "aggregates2", # max in relation argument
    "aliases",   # assignments to tuples containing variables
    "cellular_automata", # issue 198
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
    "magic_components", # component inheritance
    "magic_dfa",        # same
    "magic_dominance",  # same
    "magic_nqueens",    # recursive type
    "magic_strategies", # component inheritance
    "magic_turing1",    # issue 198
    "math",             # Trigonometric functions and FP types
    "neg1",             # issue 198
    "neg2",             # same
    "neg3",             # same
    "range",            # same
    "rec_lists",        # recursive type
    "rec_lists2",       # same
    "subsumption",      # recursive type
    "subtype",          # issue 197
    "turing1",          # issue 198
    "unused_constraints", # same
    "access-policy",    # same
    "amicable",         # same
    "array",            # same
    "catalan",          # same
    "circuit_eval",     # issue 198
    "circuit_records",  # recursive type
    "comp-parametrized", # generic component
    "comp-parametrized-inherit", # same
    "comp-parametrized-multilvl", # same
    "counter",          # issue 197
    "dfa",              # generic component
    "dfa_live_vars",    # issue 198
    "dfa_parse",        # issue 197
    "dfa_summary_function", # generic component
    "dnf",              # not in dnf form
    "dominance",        # generic component
    "edit_distance",    # issue 197
    "euclid",           # not in DNF form
    "inline_nats",      # issue 198
    "josephus",         # issue 197
    "longest_path",     # issue 198
    "magic_access-policy", # issue 197
    "nfsa2fsa",         # generic component
    "nqueens",          # recursive type
    "puzzle",           # issue 197
    "shortest_path",    # issue 197
    "sort",             # recursive type
    "strategies",       # component inheritance
    "tak",              # issue 197
    "tic-tac-toe",      # issue 197
    "weighted_distances" # issue 197
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
    code, _ = run_command(["cpp", "-P", "-undef", "-nostdinc++", f, "-o", f + ".tmp"])
    if code != 0:
        raise Exception("Error " + code + " running cpp")
    code, _ = run_command(["../../tools/souffle-converter.py", f + ".tmp", "souffle"])
    run_command(["rm", f + ".tmp"])
    if code == 0:
        code, _ = run_command(["ddlog", "-i", "souffle.dl", "-L", "../../lib"])
    if code == 0:
        tests_passed = tests_passed + 1
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
    testgroups = ["provenance", "profile", "example", "evaluation"]
    url = "https://github.com/souffle-lang/souffle/trunk/tests/"
    for tg in testgroups:
        code, dirs = run_command(["svn", "ls", url + tg])
        if code != 0:
            return
        for directory in dirs.split("/\n"):
            if directory == "":
                continue
            if directory in xfail:
                print "Expected to fail", directory
                continue
            #if not directory in todo:
            #    continue
            if not os.path.isdir(directory):
                code, _ = run_command(["svn", "export", url + tg + "/" + directory])
                if code != 0:
                    continue
            code = compile_example(directory, directory + ".dl")
            if code == 0:
                shutil.rmtree(directory)

def main():
    run_examples()
    run_remote_tests()
    print "Ran", tests_run, "out of which", tests_passed, "passed"

if __name__ == "__main__":
    main()
