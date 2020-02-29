#!/bin/bash

set -e

(cd ovn-test-data && git checkout v2)

# TODO: use OVS master once these changes are there.
(cd ovs &&
 git checkout ovn-ddlog-patches &&
 ./boot.sh &&
 ./configure  &&
 make -j6)

# TODO: maintain and eventually remove the list of tests.
# TODO: use -j6 once build script is fixed
# TODO: use primary OVN repo
(cd ovn &&
 git checkout ddlog-dev-v2 &&
 ./boot.sh &&
 ./configure --with-ddlog=../../lib --with-ovs-source=../ovs --enable-ddlog-northd-cli &&
 (make check -j1 TESTSUITEFLAGS="117-135 138-141 143-145 147-149 151-152 154-161 167-169 172 174-181 183-185 187-189 191 193-196 198-199 201-202" ||
 (find tests/testsuite.dir/ \( -name testsuite.log -o -name ovn-northd.log \) -exec echo '{}' \; -exec cat '{}' \; && false))
)

/usr/bin/time ovn/northd/ovn_northd_ddlog/target/release/ovn_northd_cli -w 2 --no-store --no-print < ovn-test-data/ovn_scale_test_short.dat > ovn_scale_test_short.dump
sed -n '/^Profile:$/,$p' ovn_scale_test_short.dump
sed -n '/Profile:/q;p' ovn_scale_test_short.dump > ovn_scale_test_short.dump.truncated
sed -n '/Profile:/q;p' ovn-test-data/ovn_scale_test_short.dump.expected > ovn_scale_test_short.dump.expected.truncated
diff -q ovn_scale_test_short.dump.truncated ovn_scale_test_short.dump.expected.truncated
