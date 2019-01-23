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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SparkConfHelperTest {

    @Test
    public void testOneGradeWhenLessTHan1GB() {
        SparkConfHelper helper = new SparkConfHelper();
        String instances = "10";
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, "1b");
        helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, instances);
        SparkConf sparkConf = new SparkConf();
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("1GB", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("1", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("512MB", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple(instances, SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("1", SparkConfHelper.SHUFFLE_PARTITIONS));
        compareConf(compareTuples, sparkConf);
    }

    @Test
    public void testTwoGradeWhenLessTHan10GB() {
        SparkConfHelper helper = new SparkConfHelper();
        String instances = "10";
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, 8L * 1024 * 1024 * 1024 + "b");
        helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, instances);
        SparkConf sparkConf = new SparkConf();
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("4GB", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("5", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("1GB", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple(instances, SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("256", SparkConfHelper.SHUFFLE_PARTITIONS));
        compareConf(compareTuples, sparkConf);
    }

    @Test
    public void testThreeGradeWhenLessHan100GB() {
        SparkConfHelper helper = new SparkConfHelper();
        String instances = "10";
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, 50L * 1024 * 1024 * 1024 + "b");
        helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, instances);
        SparkConf sparkConf = new SparkConf();
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("10GB", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("5", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("2GB", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple(instances, SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("1600", SparkConfHelper.SHUFFLE_PARTITIONS));
        compareConf(compareTuples, sparkConf);
    }

    @Test
    public void testFourGradeWhenGreaterThanOrEqual100GB() {
        SparkConfHelper helper = new SparkConfHelper();
        String instances = "10";
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, 100L * 1024 * 1024 * 1024 + "b");
        helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, instances);
        SparkConf sparkConf = new SparkConf();
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("16GB", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("5", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("4GB", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple(instances, SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("3200", SparkConfHelper.SHUFFLE_PARTITIONS));
        compareConf(compareTuples, sparkConf);
    }

    private void compareConf(List<CompareTuple> tuples, SparkConf conf) {
        for (CompareTuple tuple : tuples) {
            Assert.assertEquals(tuple.expect_Value, conf.get(tuple.key));
        }
    }

    private class CompareTuple {
        String expect_Value;
        String key;

        CompareTuple(String expect_Value, String key) {
            this.expect_Value = expect_Value;
            this.key = key;
        }
    }
}
