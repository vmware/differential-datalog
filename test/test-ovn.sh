#!/bin/bash

set -e

clone_repo() {
    REPO_URL=$1
    REPO_DIR=$2
    if [ -z "$3" ]; then
        REPO_BRANCH="master"
    else
        REPO_BRANCH=$3
    fi

    if [ ! -d "${REPO_DIR}/.git" ]
    then
        git clone -b "${REPO_BRANCH}" "${REPO_URL}" "${REPO_DIR}"
    else
        (cd "${REPO_DIR}" && git fetch "${REPO_URL}" && git checkout "${REPO_BRANCH}")
    fi
}

# When running in CI, the DDlog compiler should be prinstalled by the build stage.
if [ -z "${IS_CI_RUN}" ]; then
    stack install
fi

clone_repo https://github.com/ryzhyk/ovs.git ovs ovs-for-ddlog
clone_repo https://github.com/ryzhyk/ovn.git ovn
clone_repo https://github.com/ddlog-dev/ovn-test-data.git ovn-test-data v5

# TODO: use OVS master once these changes are there.
(cd ovs &&
 ./boot.sh &&
 ./configure  &&
 make -j6)

# TODO: maintain and eventually remove the list of tests.
# TODO: use -j6 once build script is fixed
# TODO: use primary OVN repo
(cd ovn &&
 git checkout ddlog_ci16 &&
 ./boot.sh &&
 ./configure --with-ddlog=../../lib --with-ovs-source=../ovs --enable-shared &&
 (make northd/ddlog.stamp && make check -j1 NORTHD_CLI=1 ||
 (find tests/testsuite.dir/ \( -name testsuite.log -o -name ovn-northd.log \) -exec echo '{}' \; -exec cat '{}' \; && false))
)

LD_LIBRARY_PATH=ovn/lib/.libs/:$LD_LIBRARY_PATH /usr/bin/time ovn/northd/ovn_northd_ddlog/target/release/ovn_northd_cli -w 2 --no-store < ovn-test-data/ovn_scale_test_short.dat > ovn_scale_test_short.dump
#sed -n '/^Profile:$/,$p' ovn_scale_test_short.dump
#sed -n '/Profile:/q;p' ovn_scale_test_short.dump > ovn_scale_test_short.dump.truncated
#sed -n '/Profile:/q;p' ovn-test-data/ovn_scale_test_short.dump.expected > ovn_scale_test_short.dump.expected.truncated
#diff -q ovn_scale_test_short.dump.truncated ovn_scale_test_short.dump.expected.truncated
