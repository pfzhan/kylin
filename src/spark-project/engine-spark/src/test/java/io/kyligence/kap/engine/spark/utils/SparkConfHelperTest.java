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
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.spark.SparkConf;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SparkConfHelperTest {

    @Test
    public void testUseDefaultConfWhenSmallDataSize() {
        SparkConfHelper helper = new SparkConfHelper();
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, "1");
        SparkConf sparkConf = new SparkConf();
        Map<String, String> interval = Maps.newHashMap();
        interval.put("spark.executor.memory.min", "512mb");
        interval.put("spark.executor.cores.min", "1");
        interval.put("spark.executor.memoryOverhead.min", "128mb");
        interval.put("spark.executor.instances.min", "4");
        interval.put("spark.sql.shuffle.partitions.min", "10");
        helper.generateSparkConf(interval);
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("512mb", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("1", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("128mb", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple("4", SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("10", SparkConfHelper.SHUFFLE_PARTITIONS));
        compareConf(compareTuples, sparkConf);
    }

    @Test
    public void testUseDefaultConfWhenMediumDataSize() {
        SparkConfHelper helper = new SparkConfHelper();
        String instances = "10";
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, String.valueOf(200L * 1024 * 1024 * 1024));
        helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, instances);
        SparkConf sparkConf = new SparkConf();
        Map<String, String> interval = Maps.newHashMap();
        interval.put("spark.executor.memory.max", "20000mb");
        interval.put("spark.executor.memory.min", "5000mb");
        interval.put("spark.executor.cores.max", "3");
        interval.put("spark.executor.cores.min", "1");
        interval.put("spark.executor.memoryOverhead.max", "3000mb");
        interval.put("spark.executor.memoryOverhead.min", "1000mb");
        interval.put("spark.executor.instances.max", "20");
        interval.put("spark.executor.instances.min", "5");
        interval.put("spark.sql.shuffle.partitions.max", "10000");
        interval.put("spark.sql.shuffle.partitions.min", "10");
        helper.generateSparkConf(interval);
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("10240mb", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("2", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("2560mb", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple(instances, SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("6400", SparkConfHelper.SHUFFLE_PARTITIONS));
        compareConf(compareTuples, sparkConf);
    }


    @Test
    public void testRuleBasedConfWhenLargeDataSize() {
        SparkConfHelper helper = new SparkConfHelper();
        String instances = "10";
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, String.valueOf(200L * 1024 * 1024 * 1024));
        helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, instances);
        SparkConf sparkConf = new SparkConf();
        Map<String, String> interval = Maps.newHashMap();
        interval.put("spark.executor.memory.max", "8192mb");
        interval.put("spark.executor.cores.max", "1");
        interval.put("spark.executor.memoryOverhead.max", "500mb");
        interval.put("spark.executor.instances.max", "4");
        interval.put("spark.sql.shuffle.partitions.max", "100");
        helper.generateSparkConf(interval);
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("8192mb", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("1", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("500mb", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple("4", SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("100", SparkConfHelper.SHUFFLE_PARTITIONS));
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
