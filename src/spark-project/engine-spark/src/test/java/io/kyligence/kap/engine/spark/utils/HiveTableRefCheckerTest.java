/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.utils;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.stats.utils.HiveTableRefChecker;
import org.junit.Assert;
import org.junit.Test;

public class HiveTableRefCheckerTest extends NLocalFileMetadataTestCase {

    @Test
    public void testIsNeedCleanUpTransactionalTableJob() {
        Assert.assertTrue(HiveTableRefChecker.isNeedCleanUpTransactionalTableJob(Boolean.TRUE, Boolean.FALSE, Boolean.TRUE));
        Assert.assertFalse(HiveTableRefChecker.isNeedCleanUpTransactionalTableJob(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE));
        Assert.assertTrue(HiveTableRefChecker.isNeedCleanUpTransactionalTableJob(Boolean.FALSE, Boolean.TRUE, Boolean.FALSE));
        Assert.assertTrue(HiveTableRefChecker.isNeedCleanUpTransactionalTableJob(Boolean.TRUE, Boolean.TRUE, Boolean.FALSE));
        Assert.assertFalse(HiveTableRefChecker.isNeedCleanUpTransactionalTableJob(Boolean.FALSE, Boolean.FALSE, Boolean.TRUE));
    }

    @Test
    public void testIsNeedCreateHiveTemporaryTable() {
        Assert.assertTrue(HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.TRUE, Boolean.TRUE, Boolean.TRUE));
        Assert.assertFalse(HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.FALSE, Boolean.TRUE, Boolean.FALSE));
        Assert.assertTrue(HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE));
        Assert.assertTrue(HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.FALSE, Boolean.TRUE, Boolean.TRUE));
        Assert.assertFalse(HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.FALSE, Boolean.FALSE, Boolean.TRUE));
        Assert.assertFalse(HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.FALSE, Boolean.FALSE, Boolean.FALSE));
    }

}
