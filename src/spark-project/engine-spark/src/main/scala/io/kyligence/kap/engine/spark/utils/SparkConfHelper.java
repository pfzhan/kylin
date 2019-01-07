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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.conf.rule.ExecutorCoreRule;
import org.apache.spark.conf.rule.ExecutorInstancesRule;
import org.apache.spark.conf.rule.ExecutorMemoryRule;
import org.apache.spark.conf.rule.ExecutorOverheadRule;
import org.apache.spark.conf.rule.ShufflePartitionsRule;
import org.apache.spark.conf.rule.SparkConfRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class SparkConfHelper {
    protected static final Logger logger = LoggerFactory.getLogger(SparkConfHelper.class);

    // options for helping set spark conf
    private HashMap<String, String> options = Maps.newHashMap();
    // spark configurations
    private HashMap<String, String> confs = Maps.newHashMap();

    // options key
    public static String SOURCE_TABLE_SIZE = "source_table_size";
    // configurations key
    public static String EXECUTOR_INSTANCES = "spark.executor.instances";
    public static String EXECUTOR_CORES = "spark.executor.cores";
    public static String EXECUTOR_MEMORY = "spark.executor.memory";
    public static String EXECUTOR_OVERHEAD = "spark.executor.memoryOverhead";
    public static String SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions";

    private List<SparkConfRule> EXECUTOR_RULES = ImmutableList.of(
            new ExecutorMemoryRule(),
            new ExecutorCoreRule(),
            new ExecutorOverheadRule(),
            new ExecutorInstancesRule(),
            new ShufflePartitionsRule());

    public void generateSparkConf(Map<String, String> map) {
        EXECUTOR_RULES.forEach(sparkConfRule -> sparkConfRule.apply(this, map));
    }

    public String getOption(String key) {
        return options.get(key);
    }

    public void setOption(String key, String value) {
        options.put(key, value);
    }

    public void setConf(String key, String value) {
        confs.put(key, value);
    }

    public String getConf(String key) {
        return confs.get(key);
    }

    public void applySparkConf(SparkConf sparkConf) {
        for (Map.Entry<String, String> entry : confs.entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
            logger.info("Auto set spark conf. {} = {}", entry.getKey(), entry.getValue());
        }
    }
}
