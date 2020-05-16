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
 git checkout ddlog_ci3 &&
 ./boot.sh &&
 ./configure --with-ddlog=../../lib --with-ovs-source=../ovs --enable-shared &&
 (make northd/ddlog.stamp && make check -j1 NORTHD_CLI=1 TESTSUITEFLAGS="31 33 35 37 39 41 43 45 47 49 51 53 55 57 59 61 63 65 67 69 71 73 75 77 79 81 83 85 87 89 91 93 95 97 99 101 103 105 107 109 111 113 115 117 119 121 123 125 127 129 131 133 135 137 139 141 143 145 147 149 151 153 155 157 159 161 163 165 167 169 171 175 177 179 183 185 187 189 191 193 195 197 199 205 207 209 211 213 215 217 219 221 223 225 227 231 232 234 236 238 240 242 244 246 248 250 252 254 256 258 260 262 264 266 268 270" ||
 (find tests/testsuite.dir/ \( -name testsuite.log -o -name ovn-northd.log \) -exec echo '{}' \; -exec cat '{}' \; && false))
)

LD_LIBRARY_PATH=ovn/lib/.libs/:$LD_LIBRARY_PATH /usr/bin/time ovn/northd/ovn_northd_ddlog/target/release/ovn_northd_cli -w 2 --no-store < ovn-test-data/ovn_scale_test_short.dat > ovn_scale_test_short.dump
#sed -n '/^Profile:$/,$p' ovn_scale_test_short.dump
#sed -n '/Profile:/q;p' ovn_scale_test_short.dump > ovn_scale_test_short.dump.truncated
#sed -n '/Profile:/q;p' ovn-test-data/ovn_scale_test_short.dump.expected > ovn_scale_test_short.dump.expected.truncated
#diff -q ovn_scale_test_short.dump.truncated ovn_scale_test_short.dump.expected.truncated
