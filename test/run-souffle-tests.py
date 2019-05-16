#! /usr/bin/env python2.7
"""Run various Souffle Datalog programs tests"""

import os
import subprocess
import sys
import shutil
sys.path.append(os.getcwd() + "/../tools")
from souffle_converter import convert

tests_run = 0
tests_passed = 0
tests_skipped = 0
todo = []  # if not empty only the tests in this list are run
libpath = ""

# expected to fail
xfail = [
    "souffle_tests_ddlog", # auto-generated
    "souffle7",  # recursive type
    "aggregates2", # max in relation argument
    "2sat",      # issue 197
    "aliases",   # assignments to tuples containing variables
    "cellular_automata", # issue 198
    "comp-override2", # nested component declaration
    "components1",    # improper component nesting
    "functor_arity",  # min, max, cat with more than 2 arguments
    "grammar",        # funny unicode char in a comment
    "independent_body2", # not in DNF form
    "inline_nqueens", # recursive type
    "lucas",          # inputs and outputs are in the wrong directories
    "magic_2sat",     # issue 197
    "magic_nqueens",    # recursive type
    "magic_turing1",    # issue 198
    "math",             # Trigonometric functions and FP types
    "minesweeper",      # issue 198
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
    "comp-parametrized", # issue 198
    "counter",          # issue 197
    "dfa_live_vars",    # issue 198
    "dfa_parse",        # issue 197
    "dfa_summary_function", # issue 197
    "dnf",              # not in dnf form
    "edit_distance",    # issue 197
    "euclid",           # not in DNF form
    "inline_nats",      # issue 198
    "josephus",         # issue 197
    "longest_path",     # issue 198
    "magic_access-policy", # issue 197
    "nfsa2fsa",         # recursive type
    "nqueens",          # recursive type
    "puzzle",           # issue 197
    "shortest_path",    # issue 197
    "sort",             # recursive type
    "tak",              # issue 197
    "tic-tac-toe",      # issue 197
    "weighted_distances" # issue 197
]

def run_command(command, indata=None):
    """Runs a command in a shell; returns the stdout; on error prints stderr"""
    print "Running", command
    p = subprocess.Popen(command,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate(indata)
    if p.returncode != 0 and stderr is not None:
        sys.stderr.write("{}".format(stderr))
    return p.returncode, "{}".format(stdout)

def compile_example(directory, f):
    """Run the Souffle example in directory 'directory'.  The test file is named f."""
    print "Testing " + directory
    global tests_run, tests_passed, libpath
    tests_run = tests_run + 1
    savedir = os.getcwd()
    os.chdir(directory)
    code, _ = run_command(["cpp", "-P", "-undef", "-nostdinc++", f, "-o", f + ".tmp"])
    if code != 0:
        raise Exception("Error " + str(code) + " running cpp")
    convert(f + ".tmp", "souffle", directory.replace("/", ".") + ".souffle.")

    if code == 0:
        code, _ = run_command(["ddlog", "-i", "souffle.dl", "-L", libpath])
    if code == 0:
        run_command(["rm", f + ".tmp"])
        tests_passed = tests_passed + 1
    os.chdir(savedir)
    return code

def should_run(d):
    if os.path.isfile(d):
        return False
    if d == "":
        return False
    if d in xfail:
        global tests_skipped
        tests_skipped = tests_skipped + 1
        print "Expected to fail", d
        return False
    if len(todo) > 0:
        return d in todo
    return True

def run_examples():
    """Run the hand-created examples; these are all named like 'souffle*'"""
    directories = os.listdir(".")
    for d in directories:
        if "souffle" not in d:
            continue
        if should_run(d):
            compile_example(d, "test.dl")

def normalize_name(name):
    reserved = ["match"]
    if "-" in name:
        return name.replace("-", "_")
    for r in reserved:
        if name.startswith(r + "/"):
            return name.replace(r + "/", r + "_/")
        if name.endswith("/" + r):
            return name.replace("/" + r, "/" + r + "_")
        if ("/" + r + "/") in name:
            return name.replace("/" + r + "/", "/" + r + "_/")
    return name

def run_remote_tests():
    """Run a set of tests, returns the list of the ones successfully run"""
    result = []
    testgroups = ["provenance", "profile", "example", "evaluation"]
    url = "https://github.com/souffle-lang/souffle/trunk/tests/"
    for tg in testgroups:
        code, _ = run_command(["mkdir", "-p", tg])
        if code != 0:
            return
        code, dirs = run_command(["svn", "ls", url + tg])
        if code != 0:
            return
        for test in dirs.split("/\n"):
            directory = tg + "/" + test
            if not should_run(test):
                continue
            newName = normalize_name(directory)
            print directory
            if newName != directory:
                print "Using", newName, "for", directory
            if not os.path.isdir(newName):
                os.chdir(tg)
                code, _ = run_command(["svn", "export", url + tg + "/" + test])
                os.chdir("..")
                if code != 0:
                    continue
                if newName != directory:
                    os.rename(directory, newName)
            directory = newName
            code = compile_example(directory, test + ".dl")
            if code == 0:
                result.append(directory)
            #    shutil.rmtree(directory)
    return result

def run_merged_test(filename):
    """Runs a test that encompasses multiple other tests.
    This is created in a file called souffle_tests.dl, which
    imports multiple other tests"""
    if os.path.isdir(filename + "_ddlog"):
        shutil.rmtree(filename + "_ddlog")
    code, _ = run_command(["ddlog", "-i", filename + ".dl", "-L", "../lib"])
    if code != 0:
        return
    os.chdir(filename + "_ddlog")
    code, _ = run_command(["cargo", "build", "--release"])
    if code != 0:
        return
    os.chdir("..")
    with open(filename + ".dat") as f:
        lines = f.read()
    code, result = run_command(["./" + filename + "_ddlog/target/release/" + filename +
                                "_cli", "--no-print"], lines)
    if code != 0:
        print "Error running ddlog program", code
    with open(filename + ".dump", "w") as dump:
        dump.write(result)
    code, _ = run_command(["diff", "-q", filename + ".dump", filename + ".dump.expected"])
    if code != 0:
        print "Output differs"

def main():
    global todo, tests_skipped, libpath

    path = os.getcwd()
    libpath = path + "/../lib"
    todo = sys.argv[1:]
#    run_examples()
    modules = run_remote_tests()
    print "Ran", tests_run, "out of which", tests_passed, "passed, skipped", tests_skipped
    imports = ["import " + m + ".souffle as " + m for m in modules]
    imports = [s.replace("/", ".") for s in imports]

    if len(modules) == 0:
        return

    output_file = "souffle_tests"
    with open(output_file + ".dl", "w") as testfile:
        testfile.writelines("\n".join(imports))

    # Create input script by concatenating the other input scripts
    with open(output_file + ".dat", "w") as testinputfile:
        for m in modules:
            cli_file_name = m.replace(".", "/") + "/souffle.dat"
            with open(cli_file_name, "r") as cli_file:
                for line in cli_file:
                    if line.startswith("exit"):
                        continue
                    testinputfile.write(line)
    # Create expected output by concatenating the other expected outputs
    with open(output_file + ".dump.expected", "w") as testoutputfile:
        for m in modules:
            dump_file_name = m.replace(".", "/") + "/souffle.dump.expected"
            with open(dump_file_name, "r") as dump_file:
                for line in dump_file:
                    testoutputfile.write(line)

    run_merged_test(output_file)

if __name__ == "__main__":
    main()
