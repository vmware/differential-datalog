#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
LOG_DIR="${THIS_DIR}/testsuite.log"

fail() {
    printf "${RED}FAIL${NC}"
}

ok() {
    printf "${GREEN}OK${NC}"
}

testsuite_setup() {
    mkdir -p "${LOG_DIR}"
}

gen_log_file_name() {
    log_file_name="${LOG_DIR}/${1}.log"
}

# Execute specified test function, redirecting outputs to a log file.
run_test() {
    gen_log_file_name ${1}
    if [ -z "${NO_STD_REDIRECT}" ]; then
        $1 &> "${log_file_name}"
    else
        $1
    fi
}

# List of test groups
test_groups=("crates:Test DDlog runtime crates."
             "basic:Test basic DDlog functionality."
             "perf:Performance tests."
             "go:Test Go bindings."
             "java:Test Java bindings."
             "ovn:Test OVN virtual network controller implemented in DDlog."
             "sql:Test SQL-to-DDlog compiler."
             "antrea:Test Antrea controller implemented in DDlog"
             "souffle:Tests imported from Souffle Datalog."
             "d3log:Distributed DDlog (D3log) tests."
             "misc:Miscellaneous other tests."
             "stack:Tests using Haskell stack infrastructure.")

# List of tests in each group.  Test name must match the name of a function below.

crates=("rust_fmt:Rust formatting"
        "rust_lint:Rust lints"
        "differential_datalog:Test 'differential_datalog' crate"
        "cmd_parser:Test 'cmd_parser' crate"
        "ovsdb:Test OVSDB bindings crate"
        "main_crate:Test main crate")

basic=("rust_api:Test Rust API to a DDlog program"
       "tutorial:Examples from the DDlog tutorial"
       "simple:Unit tests for various DDlog constructs"
       "simple2:Unit tests for various DDlog constructs, part 2"
       "libs:Tests for libraries in the 'lib' directory"
       "output_internal:Test '--output-internal-relations' switch"
       "stream:Test stream relations")


perf=("dcm:Declarative Cluster Management benchmark"
      "redist_opt:'redist_opt' benchmark")

go=("go_test:Go API test")

java=("java0:Java API test 0"
      "java1:Java API test 1"
      "java2:Java API test 2"
      "java3:Java API test 3"
      "java4:Java API test 4"
#      "java5:Java API test 5"  # This test fails due to #372
#      "java6:Java API test 6"  # This test coredumps
      "java7:Java API test 7"
      "flatbuf0:Java Flatbuf API test 0"
      "flatbuf1:Java Flatbuf API test 1")

sql=("sql_test:Test SQL-to-DDlog compiler")

ovn=("ovn_check:OVN controller test suite")

antrea=("antrea_check:Antrea tests")

souffle=("static_analysis:Souffle static analysis test."
         "souffle_tests1:Souffle tests part1"
         "souffle_tests2:Souffle tests part2"
         "souffle_tests3:Souffle tests part3"
         "souffle_tests4:Souffle tests part4"
         "souffle_tests5:Souffle tests part5"
         "souffle_tests6:Souffle tests part6"
         "souffle_tests7:Souffle tests part7")

d3log=("tcp_channel:TCP channel test"
       "server_api:Test D3log server API")

misc=("span_string"
      "span_uuid"
      "path:Trivial graph reachability test")

stack=("modules:Test modules and imports"
       "ovn_ftl:Test FTL syntax"
       "ovn_mockup:OVN-inspired example"
       "redist:'redist' example"
       "negative:Negative tests that validate compiler error handling")

# 'crates' test group.

rust_fmt() {
    (cd "${THIS_DIR}/rust/template/" && cargo fmt -- --check) &&
    (cd "${THIS_DIR}/rust/template/cmd_parser" && cargo fmt -- --check) &&
    (cd "${THIS_DIR}/rust/template/ovsdb" && cargo fmt -- --check) &&
    (cd "${THIS_DIR}/rust/template/differential_datalog" && cargo fmt -- --check) &&
    (cd "${THIS_DIR}/lib" && rustfmt *.rs --check)
}

rust_lint() {
    (cd "${THIS_DIR}/rust/template/" && cargo clippy --features command-line,ovsdb,c_api -- -D warnings) &&
    (cd "${THIS_DIR}/rust/template/cmd_parser" && cargo clippy -- -D warnings) &&
    (cd "${THIS_DIR}/rust/template/ovsdb" && cargo clippy -- -D warnings) &&
    (cd "${THIS_DIR}/rust/template/differential_datalog" && cargo clippy -- -D warnings)
}
differential_datalog() {
    (cd "${THIS_DIR}/rust/template/differential_datalog" && cargo test)
}

cmd_parser() {
    (cd "${THIS_DIR}/rust/template/cmd_parser" && cargo test)
}

ovsdb() {
    (cd "${THIS_DIR}/rust/template/ovsdb" && cargo test)
}

main_crate() {
    (cd "${THIS_DIR}/rust/template" && cargo test --features command-line,ovsdb,c_api)
}

# 'basic' test group.

tutorial() {
    (cd "${THIS_DIR}/test/datalog_tests" && DDLOGFLAGS="-g" ./run-test.sh tutorial release)
}

rust_api() {
    ${THIS_DIR}/test/datalog_tests/rust_api_test/test.sh
}

libs() {
    (cd "${THIS_DIR}/test/datalog_tests" && ./test-libs.sh)
}

simple() {
    (cd "${THIS_DIR}/test/datalog_tests" && DDLOGFLAGS="-g" CARGOFLAGS="--features nested_ts_32,profile,c_api" ./run-test.sh simple release)
}

simple2() {
    (cd "${THIS_DIR}/test/datalog_tests" && DDLOGFLAGS="-g --nested-ts-32" ./run-test.sh simple2 release)
}

negative() {
    (cd "${THIS_DIR}" && stack --no-terminal test --ta "-p fail")
}

output_internal() {
    (cd "${THIS_DIR}" && ./test/datalog_tests/run-tests.sh three)
}

stream() {
    (cd "${THIS_DIR}/test/datalog_tests" && ./test-stream.sh)
}

# 'perf' test group.

dcm() {
    (cd "${THIS_DIR}/test/datalog_tests" && ./test-dcm.sh)
}

redist_opt() {
    (cd "${THIS_DIR}/test/datalog_tests" && ./test-redist_opt.sh)
}

# 'go' test group

go_test() {
    (cd "${THIS_DIR}/go" && ./test.sh && ./run-example.sh)
}

# 'java' test group.

java0() {
    (cd "${THIS_DIR}/java/test" && ./run.sh)
}

java1() {
    (cd "${THIS_DIR}/java/test1" && ./run.sh)
}

java2() {
    (cd "${THIS_DIR}/java/test2" && ./run.sh)
}

java3() {
    (cd "${THIS_DIR}/java/test3" && ./run.sh)
}

java4() {
    (cd "${THIS_DIR}/java/test4" && ./run.sh)
}

java5() {
    (cd "${THIS_DIR}/java/test5" && ./run.sh)
}

java6() {
    (cd "${THIS_DIR}/java/test6" && ./run.sh)
}

java7() {
    (cd "${THIS_DIR}/java/test7" && ./run.sh)
}

flatbuf0() {
    (cd "${THIS_DIR}/java/test_flatbuf" && ./run.sh)
}

flatbuf1() {
    (cd "${THIS_DIR}/java/test_flatbuf1" && ./run.sh)
}

# 'sql' test group

sql_test() {
    (cd "${THIS_DIR}/java" && make) &&
    (export DDLOG_HOME="${THIS_DIR}" && cd "${THIS_DIR}/sql" && ./install-ddlog-jar.sh && mvn test)
}

# 'ovn' test group.

ovn_check() {
    (cd "${THIS_DIR}/test" && ./test-ovn.sh)
}

# 'antrea' test group.

antrea_check() {
    (cd "${THIS_DIR}/test/antrea" && ./test-antrea.sh)
}

# 'souffle' test group.

static_analysis() {
    (cd "${THIS_DIR}/test/souffle0" &&
     ../../tools/souffle_converter.py test.dl souffle --convert-dnf &&
     ../datalog_tests/run-test.sh souffle.dl release)
}

souffle_tests1() {
    (cd "${THIS_DIR}/test" && ./run-souffle-tests-in-batches.py 0 24)
}

souffle_tests2() {
    (cd "${THIS_DIR}/test" && ./run-souffle-tests-in-batches.py 25 49)
}

souffle_tests3() {
    (cd "${THIS_DIR}/test" && ./run-souffle-tests-in-batches.py 50 74)
}

souffle_tests4() {
    (cd "${THIS_DIR}/test" && ./run-souffle-tests-in-batches.py 75 99)
}

souffle_tests5() {
    (cd "${THIS_DIR}/test" && ./run-souffle-tests-in-batches.py 100 124)
}

souffle_tests6() {
    (cd "${THIS_DIR}/test" && ./run-souffle-tests-in-batches.py 125 149)
}

souffle_tests7() {
    (cd "${THIS_DIR}/test" && ./run-souffle-tests-in-batches.py 150 175)
}

# 'd3log' test group.

tcp_channel() {
    #ZOOKEEPER_ENDPOINTS="127.0.0.1:2181"
    #/usr/share/zookeeper/bin/zkServer.sh start &&
    (cd "${THIS_DIR}/rust/template/distributed_datalog" && cargo fmt -- --check) &&
    (cd "${THIS_DIR}/rust/template/distributed_datalog" && cargo clippy -- -D warnings) &&
    #for i in $(seq 100); do
    #    /usr/share/zookeeper/bin/zkServer.sh status && break;
    #    sleep 1
    #done &&
    (cd "${THIS_DIR}/rust/template/distributed_datalog" && (
        i=0;
        true;
        while [ $? -eq 0 -a $i -lt 100 ]; do
          i=$((i+1));
          cargo test -- tcp_channel::;
        done
        )
    )
}

server_api() {
    # It seems that stale files cause cargo to rebuild the project
    # unnecessarily in CI.
    #rm -rf test/datalog_tests/server_api_ddlog
    (export DDLOG_HOME="${THIS_DIR}" && "${THIS_DIR}/test/datalog_tests/test-server_api.sh")
}

# 'stack' test group.

modules() {
    (cd "${THIS_DIR}" && stack --no-terminal test --ta "-p modules")
}

ovn_ftl() {
    (cd "${THIS_DIR}" && stack --no-terminal test --ta "-p ovn_ftl")
}

ovn_mockup() {
    (cd "${THIS_DIR}" && stack test --ta '-p "$(NF) == \"generate ovn\" || ($(NF-1) == \"compiler tests\" && $(NF) == \"ovn\")"')
}

redist() {
    (cd "${THIS_DIR}" && STACK_CARGO_FLAGS='--release' stack test --ta '-p "$(NF) == \"generate redist\" || ($(NF-1) == \"compiler tests\" && $(NF) == \"redist\")"')
}

# 'misc' test group.

span_string() {
    (cd "${THIS_DIR}/test/datalog_tests" && ./run-test.sh span_string release)
}

span_uuid() {
    (cd "${THIS_DIR}/test/datalog_tests" && ./run-test.sh span_uuid release)
}

path() {
    (cd "${THIS_DIR}/test/datalog_tests" && ./run-test.sh path release)
}

#==========================================
# Main test script.
#==========================================

printf "DDlog test suite\n\n"
testsuite_setup

if ( [ "$#" -eq 0 ] || [ "x$1" == "xhelp" ] || [ "x$1" == "x--help" ] ); then
    printf "Usage: ${0} test_or_test_group1 test_or_test_group2 ...\n"
    printf "   or: ${0} all\n\n"
    printf "Available test groups:\n"
    for group_with_descr in "${test_groups[@]}"
    do
        IFS=":" read -ra tokens <<< "$group_with_descr"
        group=${tokens[0]}
        description=${tokens[1]}

        printf "\n    ${group}: ${description}\n"

        eval "tests=(\"\${${group}[@]}\")"
        for tst_with_descr in "${tests[@]}"
        do
            IFS=":" read -ra tokens <<< "$tst_with_descr"
            tst=${tokens[0]}
            description=${tokens[1]}
            printf "        %-25s %s\n" "${tst}" "${description}"
        done
    done
else
    if [ "x$1" == "xall" ]; then
        test_list=()
        for group_with_descr in "${test_groups[@]}"
        do
            IFS=":" read -ra tokens <<< "$group_with_descr"
            group=${tokens[0]}
            test_list+=(${group})
        done
    else
        test_list="$@"
    fi

    # Make a list of tests to run.
    all_tests=()

    for tst in "${test_list[@]}"
    do
        if [[ "${test_groups[@]}" =~ "${tst}" ]]; then
            #echo "test group '${tst}'"
            eval "tests=(\"\${${tst}[@]}\")"
            for tst_with_descr in "${tests[@]}"
            do
                IFS=":" read -ra tokens <<< "$tst_with_descr"
                tst_func=${tokens[0]}

                # Only add test if not already in the list.
                if ! [[ " ${all_tests[@]} " =~ " ${tst_func} " ]]; then
                    all_tests+=(${tst_func})
                fi
            done

        else
            # Only add test if not already in the list.
            if ! [[ " ${all_tests[@]} " =~ " ${tst} " ]]; then
                all_tests+=(${tst})
            fi
        fi
    done

    echo "Running the following tests: ${all_tests[@]}"
    echo ""

    # Validate the resulting list of tests.
    for tst in "${all_tests[@]}"; do
        if ! ([ -n "$(type -t ${tst})" ] && [ "$(type -t ${tst})" = function ]); then
            echo "Unknown test '${tst}'"
            fail=1
        fi
    done

    if [ " ${fail} " == " 1 " ]; then
        printf "${RED}FAIL${NC}\n"
        exit 1
    fi

    # Run the tests.
    passed=0
    failed=0
    for tst in "${all_tests[@]}"; do
        printf "%-25s %s" "${tst}"
        start=`date +%s`
        if run_test "${tst}"; then
            ok
            passed=$((passed+1))
        else
            fail
            if [ -z "${NO_STD_REDIRECT}" ]; then
                printf " [output saved in '${log_file_name}']"
            fi
            failed=$((failed+1))
        fi
        end=`date +%s`
        runtime=$((end-start))
        printf " (${runtime}s)\n"
    done

    if [ ${passed} -gt 0 ]; then
        echo "========================================="
        printf "${GREEN}PASSED: ${passed}${NC}\n"
        echo "========================================="
    fi

    if [ ${failed} -gt 0 ]; then
        echo "========================================="
        printf "${RED}FAILED: ${failed}${NC}\n"
        echo "========================================="
        exit 1
    fi
fi
