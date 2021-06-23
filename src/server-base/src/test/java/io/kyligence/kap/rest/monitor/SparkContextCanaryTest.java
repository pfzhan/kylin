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
package io.kyligence.kap.rest.monitor;

import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class SparkContextCanaryTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
        overwriteSystemProp("spark.local", "true");
        SparderEnv.init();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
        SparderEnv.getSparkSession().stop();
    }

    @Test
    public void testSparkKilled() {
        // first check should be good
        Assert.assertTrue(SparderEnv.isSparkAvailable());

        // stop spark and check again, the spark context should auto-restart
        SparderEnv.getSparkSession().stop();
        Assert.assertFalse(SparderEnv.isSparkAvailable());

        SparkContextCanary.getInstance().monitor();

        Assert.assertTrue(SparderEnv.isSparkAvailable());

        SparkContextCanary.getInstance().monitor();
        Assert.assertEquals(0, SparkContextCanary.getInstance().getErrorAccumulated());
    }

    @Test
    public void testSparkTimeout() {
        // first check should be GOOD
        Assert.assertTrue(SparderEnv.isSparkAvailable());

        // set kylin.canary.sqlcontext-error-response-ms to 1
        // And SparkContextCanary numberCount will timeout
        Assert.assertEquals(0, SparkContextCanary.getInstance().getErrorAccumulated());
        overwriteSystemProp("kylin.canary.sqlcontext-error-response-ms", "1");
        SparkContextCanary.getInstance().monitor();

        // errorAccumulated increase
        Assert.assertEquals(1, SparkContextCanary.getInstance().getErrorAccumulated());

        // reach threshold to restart spark. Reset errorAccumulated.
        SparkContextCanary.getInstance().monitor();
        Assert.assertEquals(2, SparkContextCanary.getInstance().getErrorAccumulated());
        SparkContextCanary.getInstance().monitor();
        Assert.assertEquals(3, SparkContextCanary.getInstance().getErrorAccumulated());

        Assert.assertTrue(SparderEnv.isSparkAvailable());
        SparderEnv.getSparkSession().stop();
    }
}
