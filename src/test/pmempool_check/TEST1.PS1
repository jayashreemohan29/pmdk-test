#
# Copyright 2014-2018, Intel Corporation
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in
#       the documentation and/or other materials provided with the
#       distribution.
#
#     * Neither the name of the copyright holder nor the names of its
#       contributors may be used to endorse or promote products derived
#       from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#
# pmempool_check/TEST1 -- test for checking pools
#

. ..\unittest\unittest.ps1

require_test_type medium

require_fs_type any

setup

$POOL="$DIR\file.pool"
$LOG="out$Env:UNITTEST_NUM.log"

echo "PMEMLOG: pool_hdr" > $LOG
expect_normal_exit $PMEMPOOL create log $POOL
check_file $POOL
# pool_hdr.features.incompat on Windows has to have SHUTDOWN_STATE feature enabled
# otherwise pmempool check will fail because of dirty shutdown state structures
# which are expected to be zeroed if SHUTDOWN_STATE feature is disabled
# 0xfe == 0xfb | SHUTDOWN_STATE
Invoke-Expression "$PMEMSPOIL -v $POOL pool_hdr.major=0x0 pool_hdr.features.compat=0xfe pool_hdr.features.incompat=0xfe pool_hdr.features.ro_compat=0xff pool_hdr.unused=ERROR >> $LOG"
expect_normal_exit $PMEMPOOL check -vry $POOL >> $LOG

echo "PMEMLOG: pmemlog" >> $LOG
rm $POOL -Force
expect_normal_exit $PMEMPOOL create log $POOL
check_file $POOL
Invoke-Expression "$PMEMSPOIL -v $POOL pmemlog.start_offset=0 pmemlog.end_offset=0 >> $LOG"
expect_normal_exit $PMEMPOOL check -vry $POOL >> $LOG

echo "PMEMBLK: pool_hdr" >> $LOG
rm $POOL -Force
expect_normal_exit $PMEMPOOL create -w blk 512 $POOL
check_file $POOL
Invoke-Expression "$PMEMSPOIL -v $POOL pool_hdr.signature=ERROR	pool_hdr.major=0xff	pool_hdr.features.compat=0xfe pool_hdr.features.incompat=0xfb pool_hdr.features.ro_compat=0xff pool_hdr.unused=ERROR >> $LOG"
expect_normal_exit $PMEMPOOL check -vry $POOL >> $LOG

echo "PMEMBLK: pmemblk" >> $LOG
rm $POOL -Force
expect_normal_exit $PMEMPOOL create -w blk 512 $POOL
check_file $POOL
Invoke-Expression "$PMEMSPOIL -v $POOL pool_hdr.signature=ERROR pmemblk.bsize=0 >> $LOG"
expect_normal_exit $PMEMPOOL check -vry $POOL >> $LOG

check

pass
