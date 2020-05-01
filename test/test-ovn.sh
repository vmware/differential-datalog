#!/bin/bash

set -e

(cd ovn-test-data && git checkout v2)

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
 git checkout ddlog_ci &&
 ./boot.sh &&
 ./configure --with-ddlog=../../lib --with-ovs-source=../ovs --enable-ddlog-northd-cli --enable-shared &&
 (make northd/ddlog.stamp && make check -j1 TESTSUITEFLAGS="31 33 37 39 41 43 45 47 49 51 53 55 57 59 61 63 65 67 69 71 73 75 77 79 81 83 85 87 89 91 93 95 97 99 101 103 105 107 109 111 113 115 117 119 121 123 125 127 129 131 133 135 137 139 141 143 145 147 149 151 153 155 157 159 161 163 165 167 169 171 173 175 177 179 181 183 185 187 189 191 193 195 197 199 201 205 207 209 211 213 215 217 219 221 225 227 229 234 236 238 240 242 244 254 256 258 259 260 261 262 263 264 265 266 267 268 269 270 271 272 273 274 275 276 277 278 279 280 281 282 283 284 285 286 287 288 289 290 291 292 293 294 295 296 297 298 299 300 301 302 303 304 305 306 307 308 309 310 311 312 313 314 315" ||
 (find tests/testsuite.dir/ \( -name testsuite.log -o -name ovn-northd.log \) -exec echo '{}' \; -exec cat '{}' \; && false))
)

/usr/bin/time ovn/northd/ovn_northd_ddlog/target/release/ovn_northd_cli -w 2 --no-store --no-print < ovn-test-data/ovn_scale_test_short.dat > ovn_scale_test_short.dump
sed -n '/^Profile:$/,$p' ovn_scale_test_short.dump
sed -n '/Profile:/q;p' ovn_scale_test_short.dump > ovn_scale_test_short.dump.truncated
sed -n '/Profile:/q;p' ovn-test-data/ovn_scale_test_short.dump.expected > ovn_scale_test_short.dump.expected.truncated
diff -q ovn_scale_test_short.dump.truncated ovn_scale_test_short.dump.expected.truncated
