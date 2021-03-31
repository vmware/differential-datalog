#! /usr/bin/env python3
"""Run various Souffle Datalog programs tests
   Returns 0 when no errors occurred, 1 when some error occurs, and 2 when no tests are run"""

# pylint: disable=invalid-name,missing-docstring,global-variable-not-assigned,too-many-locals,too-many-return-statements

import os
import subprocess
import sys
import argparse
import datetime

sys.path.append(os.getcwd() + "/../tools")
from souffle_converter import convert, ConversionOptions

souffle_release = "2.0.2"  # Run tests from this release of Souffle
verbose = False
tests_compiled = 0
tests_compiled_successfully = 0
tests_xfail = 0
todo = []  # if not empty only the tests in this list are run
libpath = ""
tests_to_skip = 0 # Skip this many tests
tests_to_run = 20 # number of tests to run at once in Rust

# expected to fail
xfail = [
    "arithm", # cannot infer argument type in pow32(2)
    "souffle_tests_ddlog", # auto-generated
    "souffle7",  # issue 202 - recursive type
    "aggregates2", # aggregation used in head
    "aggregates5", # 198
    "aggregates7", # pow32 issue
    "aggregates_complex", # cannot be evaluated bottom-up; issue 293
    "aggregates_non_materialised", # 198
    "aggregates",  # issue 227 - count of empty group
    "aggregates_nested", # ??
    "indexed_inequalities", # DDlog cannot infer the type of `2` in `2^32`.
    "magic_aggregates",  # 227
    "count",       # 227
    "choice_total_order", # Takes too much time
    "magic_count", # 227
    "magic_samegen", # computes a very large join
    "circuit_sat", # issue 221 - starting state is very large
    "magic_circuit_sat", # 221
    "factorial",   # 221
    "grid",        # 221
    "2sat",      # issue 197 - bind and use variable
    "floydwarshall", # 197
    "independent_body3", # issue 224 - requires many iterations
    "aliases",   # assignments to tuples containing variables
    "cellular_automata", # issue 198 - order of clauses
    "comp-override2", # nested component declaration
    "components1",    # improper component nesting
    "independent_body2", # issue #231 - not in DNF form, #197
    "lucas",          # inputs and outputs are in the wrong directories
    "magic_2sat",     # 197
    "magic_nqueens",    # 202
    "inline_nqueens",   # 202
    "magic_turing1",    # 198
    "magic_bindings",   # issue 198
    "magic_infbinding", # issue 198
    "math",             # Missing reference outputs in Souffle
    "minesweeper",      # 198
    "neg1",             # 198
    "neg2",             # 198
    "neg3",             # 198
    "range",            # 198
    "rec_lists",        # 202
    "rec_lists2",       # 202
    "rewrite",          # 202
    "subsumption",      # 202
    "subtype",          # 197
    "turing1",          # 198
    "unused_constraints", # 198
    "access-policy",    # 198
    "amicable",         # 198
    "array",            # 198
    "catalan",          # 198
    "circuit_eval",     # 198
    "circuit_records",  # 202
    "comp-parametrized", # 198
    "counter",          # 197
    "dfa_live_vars",    # 198
    "dfa_parse",        # 197
    "dfa_summary_function", # 197
    "dnf",              # 231
    "edit_distance",    # 197
    "euclid",           # 231, 202
    "inline_nats",      # 198
    "josephus",         # 197
    "longest_path",     # 198
    "magic_access-policy", # 197
    "nfsa2fsa",         # 202
    "nqueens",          # 202
    "puzzle",           # 197
    "shortest_path",    # 197
    "sort",             # 202
    "tak",              # 197
    "tic-tac-toe",      # 197
    "weighted_distances", # 197
    "aggregate_witnesses" # aggregate does not assign to variable
]

def exit(code):
    if code != 0:
        print("Terminating with exit code", str(code))
    sys.exit(code)

def run_command(command, indata=None):
    """Runs a command in a shell; returns the stdout; on error prints stderr"""
    # print("Running", command)
    p = subprocess.Popen(command,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate(indata)
    if p.returncode != 0 and stderr is not None:
        sys.stderr.write("{}".format(stderr.decode()))
    return p.returncode, "{}".format(stdout.decode())

def compile_example(directory, f):
    """Run the Souffle example in directory 'directory'.  The test file is named f.
       Returns the exit code of the compilation command."""
    print("Testing", directory)
    global tests_compiled, tests_compiled_successfully, libpath
    tests_compiled = tests_compiled + 1
    savedir = os.getcwd()
    os.chdir(directory)
    code, output = run_command(["cc", "-x", "c", "-E", "-P", "-undef", "-nostdinc", f])
    if code != 0:
        raise Exception("Error " + str(code) + " running cpp")
    with open(f + ".tmp", "w") as tmp:
        tmp.write(output)
    options = ConversionOptions()
    options.relationPrefix = directory.replace("/", "::") + "::souffle::"
    options.toDNF = True
    convert(f + ".tmp", "souffle", options)

    if code == 0:
        code, _ = run_command(["ddlog", "-i", "souffle.dl", "-L", libpath])
    if code == 0:
        run_command(["rm", f + ".tmp"])
        tests_compiled_successfully = tests_compiled_successfully + 1
    else:
        print("Error compiling", directory + "/" + f)
    os.chdir(savedir)
    return code

def should_run(d):
    if os.path.isfile(d):
        return False
    if d == "":
        return False
    if d in xfail:
        global tests_xfail
        tests_xfail = tests_xfail + 1
        if verbose:
            print("Expected to fail", d)
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
            code = compile_example(d, "test.dl")
            if code != 0:
                return code
    return 0

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
    testgroups = ["example", "evaluation"]
    global souffle_release
    url = "https://github.com/souffle-lang/souffle/tags/" + souffle_release + "/tests/"
    for tg in testgroups:
        code, _ = run_command(["mkdir", "-p", tg])
        if code != 0:
            return []
        code, dirs = run_command(["svn", "ls", url + tg])
        if code != 0:
            return []
        for test in dirs.split("/\n"):
            directory = tg + "/" + test
            if not should_run(test):
                continue

            global tests_to_skip, verbose
            if tests_to_skip > 0:
                tests_to_skip = tests_to_skip - 1
                if verbose:
                    print("Skipping", directory)
                continue

            newName = normalize_name(directory)
            if newName != directory:
                print("Using", newName, "for", directory)

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
            else:
                exit(1)
            global tests_compiled_successfully, tests_to_run
            if tests_compiled_successfully == tests_to_run:
                return result
    return result

def run_merged_test(filename):
    """Runs a test that encompasses multiple other tests.
    This is created in a file called souffle_tests.dl, which
    imports multiple other tests"""
    code, _ = run_command(["ddlog", "-i", filename + ".dl", "-L",
                           "../lib", "--no-dynlib", "--no-staticlib"])
    if code != 0:
        print("*** Error in compiling")
        return code
    os.chdir(filename + "_ddlog")
    print("Compiling Rust at", datetime.datetime.now().time())
    code, _ = run_command(["cargo", "build"])
    if code != 0:
        return code
    os.chdir("..")
    with open(filename + ".dat", 'r') as f:
        lines = f.read()
    print("Running program at", datetime.datetime.now().time())
    code, result = run_command(["./" + filename + "_ddlog/target/debug/" + filename +
                                "_cli", "--no-init-snapshot"], lines.encode())
    if code != 0:
        print("Error running ddlog program; exit code", code)
    with open(filename + ".dump", "w") as dump:
        dump.write(result)
    run_command(["sort", filename + ".dump", "-o", "tmp.sorted"])
    run_command(["mv", "tmp.sorted", filename + ".dump"])
    run_command(["sort", filename + ".dump.expected", "-o", "tmp.sorted"])
    run_command(["mv", "tmp.sorted", filename + ".dump.expected"])
    code, _ = run_command(["diff", "-q", filename + ".dump", filename + ".dump.expected"])
    if code != 0:
        print("*** Error: Output differs: " + filename + ".dump[.expected]")
    print("Completed program at", datetime.datetime.now().time())
    return code

def main():
    os.environ["RUSTFLAGS"] = "-Awarnings" # " -opt-level=z"
    global todo, tests_xfail, libpath, verbose
    print("Starting at", datetime.datetime.now().time())
    argParser = argparse.ArgumentParser(
        "run-souffle-tests.py",
        description="Runs a number of Datalog Souffle tests from github.")
    argParser.add_argument("-s", "--skip", help="Number of tests to skip")
    argParser.add_argument("-v", "--verbose", help="Verbose operation",
                           action="store_true")
    argParser.add_argument("-r", "--run", help="Number of tests to run")
    argParser.add_argument("-e", "--examples", action="store_true",
                           help="Run the examples without reference data")
    argParser.add_argument("remaining", nargs="*")
    args = argParser.parse_args()
    verbose = args.verbose

    run_command(["stack", "install"])
    path = os.getcwd()
    libpath = path + "/../lib"
    todo = args.remaining
    if args.examples:
        code = run_examples()
        if code != 0:
            exit(1)

    global tests_to_skip, tests_to_run, tests_compiled_successfully, tests_compiled
    if args.skip is not None:
        tests_to_skip = int(args.skip)
    if args.run is not None:
        tests_to_run = int(args.run)
    save_skip = tests_to_skip
    modules = run_remote_tests()
    print("Converted successfully", tests_compiled_successfully, "out of", \
        tests_compiled, "skipped xfail", tests_xfail, \
        "running", tests_to_run, "after skipping", save_skip)
    imports = ["import " + m + "::souffle as " + m for m in modules]
    imports = [s.replace("/", "::") for s in imports]

    if len(modules) == 0:
        exit(2)

    output_file = "souffle_tests"
    with open(output_file + ".dl", "w") as testfile:
        testfile.writelines("\n".join(imports))

    # Create input script by concatenating the other input scripts
    with open(output_file + ".dat", "w") as testinputfile:
        for m in modules:
            cli_file_name = m.replace("::", "/") + "/souffle.dat"
            with open(cli_file_name, "r") as cli_file:
                for line in cli_file:
                    if line.startswith("exit"):
                        continue
                    testinputfile.write(line)
        testinputfile.write("exit")

    # Create expected output by concatenating the other expected outputs
    with open(output_file + ".dump.expected", "w") as testoutputfile:
        for m in modules:
            dump_file_name = m.replace("::", "/") + "/souffle.dump.expected"
            with open(dump_file_name, "r") as dump_file:
                for line in dump_file:
                    testoutputfile.write(line)

    code = run_merged_test(output_file)
    if code != 0:
        exit(1)
    exit(0)

if __name__ == "__main__":
    main()
