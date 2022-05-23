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
package io.kyligence.kap.query.engine;


import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.common.util.TempMetadataBuilder;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.KylinSession$;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SparderInitSQLConfTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetOrCreateKylinSession() {
        SparkConf sparkConf = getSparkConf();
        SparkSession.Builder sessionBuilder = SparkSession.builder()
                .enableHiveSupport().config(sparkConf)
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        SparkSession session = KylinSession$.MODULE$.KylinBuilder(sessionBuilder).buildCluster().getOrCreateKylinSession();
        Assert.assertTrue(session.sessionState().conf().ansiEnabled());
        Assert.assertTrue(SQLConf.get().ansiEnabled());
        session.close();
    }

    @Test
    public void testCloneSession() {
        SparkConf sparkConf = getSparkConf();
        SparkSession.Builder sessionBuilder = SparkSession.builder()
                .enableHiveSupport().config(sparkConf)
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        SparkSession session = KylinSession$.MODULE$.KylinBuilder(sessionBuilder).buildCluster().getOrCreateKylinSession();
        session = session.cloneSession();
        Assert.assertTrue(session.sessionState().conf().ansiEnabled());
        Assert.assertTrue(SQLConf.get().ansiEnabled());
        session.close();
    }

    @Test
    public void testNewSession() {
        SparkConf sparkConf = getSparkConf();
        SparkSession.Builder sessionBuilder = SparkSession.builder()
                .enableHiveSupport().config(sparkConf)
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
        SparkSession session = KylinSession$.MODULE$.KylinBuilder(sessionBuilder).buildCluster().getOrCreateKylinSession();
        session = session.newSession();
        Assert.assertTrue(session.sessionState().conf().ansiEnabled());
        Assert.assertTrue(SQLConf.get().ansiEnabled());
        session.close();
    }

    public SparkConf getSparkConf() {
        SparkConf sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[1]");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");
        sparkConf.set("spark.sql.ansi.enabled", "true");
        return sparkConf;
    }

}
