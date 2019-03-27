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

package io.kyligence.kap.engine.spark.application;

import io.kyligence.kap.common.obf.IKeep;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import io.kyligence.kap.spark.common.CredentialUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Application;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.job.UdfManager;
import io.kyligence.kap.engine.spark.utils.JobMetricsUtils;
import io.kyligence.kap.engine.spark.utils.SparkConfHelper;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import lombok.val;

public abstract class SparkApplication implements Application, IKeep {
    private static final Logger logger = LoggerFactory.getLogger(SparkApplication.class);
    private Map<String, String> params = Maps.newHashMap();
    protected volatile KylinConfig config;
    protected volatile String jobId;
    protected SparkSession ss;
    protected String project;

    public void execute(String[] args) {
        try {
            String argsLine = Files.readAllLines(Paths.get(args[0])).get(0);
            if (argsLine.isEmpty()) {
                throw new RuntimeException("Args file is empty");
            }
            params = JsonUtil.readValueAsMap(argsLine);
            checkArgs();
            logger.info("Executor task " + this.getClass().getName() + " with args : " + argsLine);
            execute();
        } catch (Exception e) {
            throw new RuntimeException("Error execute " + this.getClass().getName(), e);
        }
    }

    public final Map<String, String> getParams() {
        return this.params;
    }

    public final String getParam(String key) {
        return this.params.get(key);
    }

    public final void setParam(String key, String value) {
        this.params.put(key, value);
    }

    public final boolean contains(String key) {
        return params.containsKey(key);
    }

    public void checkArgs() {
        // do nothing
    }

    final protected void execute() throws Exception {
        String hdfsMetalUrl = getParam(NBatchConstants.P_DIST_META_URL);
        jobId = getParam(NBatchConstants.P_JOB_ID);
        project = getParam(NBatchConstants.P_PROJECT_NAME);
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoCloseConfig = KylinConfig
                .setAndUnsetThreadLocalConfig(KylinConfig.loadKylinConfigFromHdfs(hdfsMetalUrl))) {
            config = autoCloseConfig.get();
            SparkConf sparkConf = new SparkConf();
            if (config.isAutoSetSparkConf() && isAutoSetSparkConfEnabled()) {
                try {
                    autoSetSparkConf(sparkConf);
                } catch (Exception e) {
                    logger.warn("Auto set spark conf failed. Load spark conf from system properties", e);
                    sparkConf = new SparkConf();
                }
            }
            // for wrapping credential
            CredentialUtils.wrap(sparkConf, project);

            ss = SparkSession.builder().enableHiveSupport().config(sparkConf)
                    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false").getOrCreate();

            // for spark metrics
            JobMetricsUtils.registerListener(ss);

            //#8341
            SparderEnv.setSparkSession(ss);
            UdfManager.create(ss);

            if (!config.isUTEnv()) {
                System.setProperty("kylin.env", config.getDeployEnv());
            }
            doExecute();
            // Output metadata to another folder
            val resourceStore = ResourceStore.getKylinMetaStore(config);
            val outputConfig = KylinConfig.createKylinConfig(config);
            outputConfig.setMetadataUrl(getParam(NBatchConstants.P_OUTPUT_META_URL));
            ResourceStore.createMetadataStore(outputConfig).dump(resourceStore);
        } finally {
            if (ss != null && !ss.conf().get("spark.master").startsWith("local")) {
                JobMetricsUtils.unRegisterListener(ss);
                ss.stop();
            }
        }
    }

    public boolean isAutoSetSparkConfEnabled() {
        return true;
    }

    protected abstract void doExecute() throws Exception;

    private void autoSetSparkConf(SparkConf sparkConf) throws Exception {
        logger.info("Start set spark conf automatically.");
        SparkConfHelper helper = new SparkConfHelper();
        Path shareDir = config.getJobTmpShareDir(project, jobId);
        String contentSize = chooseContentSize(shareDir);
        // add content size with unit
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, contentSize);
        Map<String, String> configOverride = config.getSparkConfigOverride();
        Preconditions.checkState(configOverride.containsKey(SparkConfHelper.EXECUTOR_INSTANCES));
        helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, configOverride.get(SparkConfHelper.EXECUTOR_INSTANCES));
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
    }

    protected String chooseContentSize(Path shareDir) throws IOException {
        FileSystem fs = HadoopUtil.getFileSystem(shareDir);
        FileStatus[] fileStatuses = fs.listStatus(shareDir, path -> path.toString().endsWith(ResourceDetectUtils.fileName()));
        Map<String, List<String>> resourcePaths = Maps.newHashMap();
        for (FileStatus file : fileStatuses) {
            String fileName = file.getPath().getName();
            String segmentId = fileName.substring(0, fileName.indexOf(ResourceDetectUtils.fileName()));
            Map<String, List<String>> map = ResourceDetectUtils.readResourcePaths(file.getPath());
            for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                resourcePaths.put(segmentId + entry.getKey(), entry.getValue());
            }
        }
        // return size with unit
        return ResourceDetectUtils.getMaxResourceSize(resourcePaths) + "b";
    }
}
