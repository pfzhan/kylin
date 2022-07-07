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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.conf.rule.ExecutorCoreRule;
import org.apache.spark.conf.rule.ExecutorInstancesRule;
import org.apache.spark.conf.rule.ExecutorMemoryRule;
import org.apache.spark.conf.rule.ExecutorOverheadRule;
import org.apache.spark.conf.rule.ShufflePartitionsRule;
import org.apache.spark.conf.rule.SparkConfRule;
import org.apache.spark.conf.rule.StandaloneConfRule;
import org.apache.spark.conf.rule.YarnConfRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.cluster.IClusterManager;
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;

public class SparkConfHelper {
    protected static final Logger logger = LoggerFactory.getLogger(SparkConfHelper.class);

    // options for helping set spark conf
    private HashMap<String, String> options = Maps.newHashMap();
    // spark configurations
    private HashMap<String, String> confs = Maps.newHashMap();

    private IClusterManager clusterManager;

    // options key
    public static final String SOURCE_TABLE_SIZE = "source_table_size";
    public static final String LAYOUT_SIZE = "layout_size";
    // configurations key
    public static final String DEFAULT_QUEUE = "spark.yarn.queue";
    public static final String REQUIRED_CORES = "required_cores";
    public static final String EXECUTOR_INSTANCES = "spark.executor.instances";
    public static final String EXECUTOR_CORES = "spark.executor.cores";
    public static final String EXECUTOR_MEMORY = "spark.executor.memory";
    public static final String EXECUTOR_OVERHEAD = "spark.executor.memoryOverhead";
    public static final String SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions";
    public static final String DRIVER_MEMORY = "spark.driver.memory";
    public static final String DRIVER_OVERHEAD = "spark.driver.memoryOverhead";
    public static final String DRIVER_CORES = "spark.driver.cores";
    public static final String MAX_CORES = "spark.cores.max";
    public static final String COUNT_DISTICT = "count_distinct";

    private static final List<SparkConfRule> EXECUTOR_RULES = Lists.newArrayList(new ExecutorMemoryRule(),
            new ExecutorCoreRule(), new ExecutorOverheadRule(), new ExecutorInstancesRule(),
            new ShufflePartitionsRule(), new StandaloneConfRule(), new YarnConfRule());

    public void generateSparkConf() {
        KylinConfig.getInstanceFromEnv().getSparkBuildConfExtraRules()
                .forEach(rule -> EXECUTOR_RULES.add((SparkConfRule) ClassUtil.newInstance(rule)));
        EXECUTOR_RULES.forEach(sparkConfRule -> sparkConfRule.apply(this));
    }

    public String getOption(String key) {
        return options.getOrDefault(key, null);
    }

    public void setOption(String key, String value) {
        options.put(key, value);
    }

    public void setConf(String key, String value) {
        confs.put(key, value);
    }

    public String getConf(String key) {
        return confs.getOrDefault(key, null);
    }

    public IClusterManager getClusterManager() {
        return clusterManager;
    }

    public void setClusterManager(IClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void applySparkConf(SparkConf sparkConf) throws JsonProcessingException {
        KylinBuildEnv.get().buildJobInfos().recordAutoSparkConfs(confs);
        logger.info("Auto set spark conf: {}", JsonUtil.writeValueAsString(confs));
        for (Map.Entry<String, String> entry : confs.entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
        }
    }

    public boolean hasCountDistinct() {
        return "true".equalsIgnoreCase(getConf(COUNT_DISTICT));
    }
}
