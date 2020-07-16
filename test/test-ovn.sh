#!/bin/bash

set -e

(cd ovn-test-data && git checkout v4)

# TODO: use OVS master once these changes are there.
(cd ovs &&
 git checkout ovs-for-ddlog &&
 ./boot.sh &&
 ./configure  &&
 make -j6)

# TODO: maintain and eventually remove the list of tests.
# TODO: use -j6 once build script is fixed
# TODO: use primary OVN repo
(cd ovn &&
 git checkout ddlog_ci6 &&
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
