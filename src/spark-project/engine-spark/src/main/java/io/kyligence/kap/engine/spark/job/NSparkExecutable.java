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

package io.kyligence.kap.engine.spark.job;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.engine.spark.merger.MetadataMerger;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

/**
 */
public class NSparkExecutable extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkExecutable.class);

    void setDataflowId(String dataflowId) {
        this.setParam(NBatchConstants.P_DATAFLOW_ID, dataflowId);
    }

    public String getDataflowId() {
        return this.getParam(NBatchConstants.P_DATAFLOW_ID);
    }

    void setJobId(String jobId) {
        this.setParam(NBatchConstants.P_JOB_ID, jobId);
    }

    void setSegmentIds(Set<String> segmentIds) {
        this.setParam(NBatchConstants.P_SEGMENT_IDS, Joiner.on(",").join(segmentIds));
    }

    public Set<String> getSegmentIds() {
        return Sets.newHashSet(StringUtils.split(this.getParam(NBatchConstants.P_SEGMENT_IDS), ","));
    }

    void setCuboidLayoutIds(Set<Long> clIds) {
        this.setParam(NBatchConstants.P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(clIds));
    }

    public Set<Long> getCuboidLayoutIds() {
        return NSparkCubingUtil.str2Longs(this.getParam(NBatchConstants.P_LAYOUT_IDS));
    }

    protected void setProjectParam(String project) {
        this.setParam(NBatchConstants.P_PROJECT_NAME, project);
    }

    protected void setSparkSubmitClassName(String className) {
        this.setParam(NBatchConstants.P_CLASS_NAME, className);
    }

    public String getSparkSubmitClassName() {
        return this.getParam(NBatchConstants.P_CLASS_NAME);
    }

    protected void setJars(String jars) {
        this.setParam(NBatchConstants.P_JARS, jars);
    }

    public String getJars() {
        return this.getParam(NBatchConstants.P_JARS);
    }

    protected void setDistMetaUrl(String metaUrl) {
        this.setParam(NBatchConstants.P_DIST_META_URL, metaUrl);
        this.setParam(NBatchConstants.P_OUTPUT_META_URL, metaUrl + "_output");
    }

    public String getDistMetaUrl() {
        return this.getParam(NBatchConstants.P_DIST_META_URL);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final KylinConfig config = wrapConfig(context);

        String sparkHome = KylinConfig.getSparkHome();
        if (StringUtils.isEmpty(sparkHome) && !config.isUTEnv()) {
            throw new RuntimeException("Missing spark home");
        }

        String kylinJobJar = config.getKylinJobJarPath();
        if (StringUtils.isEmpty(kylinJobJar) && !config.isUTEnv()) {
            throw new RuntimeException("Missing kylin job jar");
        }

        String hadoopConf = System.getProperty("kylin.hadoop.conf.dir");
        if (StringUtils.isEmpty(hadoopConf) && !config.isUTEnv()) {
            throw new RuntimeException(
                    "kylin_hadoop_conf_dir is empty, check if there's error in the output of 'kylin.sh start'");
        }

        File hiveConfFile = new File(hadoopConf, "hive-site.xml");
        if (!hiveConfFile.exists() && !config.isUTEnv()) {
            throw new RuntimeException("Cannot find hive-site.xml in kylin_hadoop_conf_dir: " + hadoopConf + //
                    ". In order to enable spark cubing, you must set kylin.env.hadoop-conf-dir to a dir which contains at least core-site.xml, hdfs-site.xml, hive-site.xml, mapred-site.xml, yarn-site.xml");
        }

        String jars = getJars();
        if (StringUtils.isEmpty(jars)) {
            jars = kylinJobJar;
        }

        deleteJobTmpDirectoryOnExists();

        try {
            attachMetadataAndKylinProps(config);
        } catch (IOException e) {
            throw new ExecuteException("meta dump failed", e);
        }
        String filePath = dumpArgs();
        if (config.isUTEnv()) {
            return runLocalMode(filePath);
        } else {
            killOrphanApplicationIfExists(config);
            return runSparkSubmit(config, sparkHome, hadoopConf, jars, kylinJobJar,
                    "-className " + getSparkSubmitClassName() + " " + filePath, getParent().getId());
        }
    }

    String dumpArgs() throws ExecuteException {
        File tmpDir = null;
        try {
            tmpDir = File.createTempFile(NBatchConstants.P_LAYOUT_IDS, "");
            FileUtils.writeByteArrayToFile(tmpDir, JsonUtil.writeValueAsBytes(getParams()));

            logger.info("Spark job args json is : {}.", JsonUtil.writeValueAsString(getParams()));
            return tmpDir.getCanonicalPath();
        } catch (IOException e) {
            if (tmpDir != null && tmpDir.exists()) {
                try {
                    Files.delete(tmpDir.toPath());
                } catch (IOException e1) {
                    throw new ExecuteException(
                            "Write cuboidLayoutIds failed: Error for delete file " + tmpDir.getPath(), e1);
                }
            }
            throw new ExecuteException("Write cuboidLayoutIds failed: ", e);
        }
    }

    protected KylinConfig wrapConfig(ExecutableContext context) {
        val originalConfig = context.getConfig();
        KylinConfigExt kylinConfigExt = null;
        val project = getProject();
        Preconditions.checkState(StringUtils.isNotBlank(project), "job " + getId() + " project info is empty");
        val dataflow = getParam(NBatchConstants.P_DATAFLOW_ID);
        boolean needCloseAutoConf = false;
        if (StringUtils.isNotBlank(dataflow)) {
            val dataflowManager = NDataflowManager.getInstance(originalConfig, project);
            kylinConfigExt = dataflowManager.getDataflow(dataflow).getConfig();
            needCloseAutoConf = hasOverrideSparkConf(dataflow, dataflowManager);
        } else {
            val projectInstance = NProjectManager.getInstance(originalConfig).getProject(project);
            kylinConfigExt = projectInstance.getConfig();
        }

        val jobOverrides = Maps.<String, String> newHashMap();
        val parentId = getParentId();
        jobOverrides.put("job.id", StringUtils.defaultIfBlank(parentId, getId()));
        jobOverrides.put("job.project", project);
        if (StringUtils.isNotBlank(parentId)) {
            jobOverrides.put("job.stepId", getId());
        }
        if (needCloseAutoConf) {
            jobOverrides.put("kylin.spark-conf.auto.prior", "false");
        }
        jobOverrides.putAll(kylinConfigExt.getExtendedOverrides());
        return KylinConfigExt.createInstance(kylinConfigExt, jobOverrides);
    }

    private boolean hasOverrideSparkConf(String dataflow, NDataflowManager dataflowManager) {
        LinkedHashMap<String, String> overrideProps = dataflowManager.getDataflow(dataflow).getIndexPlan()
                .getOverrideProps();
        Optional<Entry<String, String>> any = overrideProps.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("kylin.engine.spark-conf."))
                .filter(entry -> !entry.getKey().equals("kylin.engine.spark-conf.spark.executor.instances")).findAny();
        if (any.isPresent()) {
            logger.info(
                    "Find override spark conf except spark.executor.instances, set kylin.spark-conf.auto.prior=false");
            overrideProps.entrySet()
                    .forEach(entry -> logger.info("Override conf :{}={}", entry.getKey(), entry.getValue()));
            return true;
        }
        return false;
    }

    private void killOrphanApplicationIfExists(KylinConfig config) {
        PatternedLogger patternedLogger = new PatternedLogger(logger);
        String orphanApplicationId = null;

        try (YarnClient yarnClient = YarnClient.createYarnClient()) {
            Configuration yarnConfiguration = new YarnConfiguration();
            // bug of yarn : https://issues.apache.org/jira/browse/SPARK-15343
            yarnConfiguration.set("yarn.timeline-service.enabled", "false");
            yarnClient.init(yarnConfiguration);
            yarnClient.start();

            Set<String> types = Sets.newHashSet("SPARK");
            EnumSet<YarnApplicationState> states = EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
                    YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING);
            val applicationReports = yarnClient.getApplications(types, states);

            if (CollectionUtils.isEmpty(applicationReports))
                return;

            for (ApplicationReport report : applicationReports) {
                if (report.getName().equalsIgnoreCase("job_step_" + getId())) {
                    orphanApplicationId = report.getApplicationId().toString();
                    // kill orphan application by command line
                    String killApplicationCmd = "yarn application -kill " + orphanApplicationId;
                    config.getCliCommandExecutor().execute(killApplicationCmd, patternedLogger);
                }
            }
        } catch (ShellException ex1) {
            logger.error("kill orphan yarn application {} failed.", orphanApplicationId);
        } catch (YarnException | IOException ex2) {
            logger.error("get yarn application failed");
        }
    }

    private ExecuteResult runSparkSubmit(KylinConfig config, String sparkHome, String hadoopConf, String jars,
            String kylinJobJar, String appArgs, String jobId) {

        PatternedLogger patternedLogger = new PatternedLogger(logger);
        try {
            String cmd = generateSparkCmd(config, hadoopConf, jars, kylinJobJar, appArgs);

            CliCommandExecutor exec = new CliCommandExecutor();
            Pair<Integer, String> result = exec.execute(cmd, patternedLogger, jobId);

            Preconditions.checkState(result.getFirst() == 0);
            Map<String, String> extraInfo = makeExtraInfo(patternedLogger.getInfo());
            val ret = ExecuteResult.createSucceed(result.getSecond());
            ret.getExtraInfo().putAll(extraInfo);
            return ret;
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        return config.getSparkConfigOverride();
    }

    protected String generateSparkCmd(KylinConfig config, String hadoopConf, String jars, String kylinJobJar,
            String appArgs) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "export HADOOP_CONF_DIR=%s && %s/bin/spark-submit --class io.kyligence.kap.engine.spark.application.SparkEntry ");

        Map<String, String> sparkConfs = getSparkConfigOverride(config);
        for (Entry<String, String> entry : sparkConfs.entrySet()) {
            appendSparkConf(sb, entry.getKey(), entry.getValue());
        }
        appendSparkConf(sb, "spark.executor.extraClassPath", Paths.get(kylinJobJar).getFileName().toString());

        sb.append("--name job_step_%s ");
        sb.append("--jars %s %s %s");
        String cmd = String.format(sb.toString(), hadoopConf, KylinConfig.getSparkHome(), getId(), jars, kylinJobJar,
                appArgs);
        logger.info("spark submit cmd: {}", cmd);
        return cmd;
    }

    protected void appendSparkConf(StringBuilder sb, String key, String value) {
        // Multiple parameters in "--conf" need to be enclosed in single quotes
        sb.append(" --conf '").append(key).append("=").append(value).append("' ");
    }

    private ExecuteResult runLocalMode(String appArgs) {
        try {
            Class<? extends Object> appClz = ClassUtil.forName(getSparkSubmitClassName(), Object.class);
            appClz.getMethod("main", String[].class).invoke(null, (Object) new String[] { appArgs });
            return ExecuteResult.createSucceed();
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    protected Set<String> getMetadataDumpList(KylinConfig config) {
        return Collections.emptySet();
    }

    private void attachMetadataAndKylinProps(KylinConfig config) throws IOException {
        Set<String> dumpList = getMetadataDumpList(config);
        if (dumpList.isEmpty()) {
            return;
        }

        String metaDumpUrl = getDistMetaUrl();
        if (StringUtils.isEmpty(metaDumpUrl)) {
            throw new RuntimeException("Missing metaUrl");
        }

        File tmpDir = File.createTempFile("kylin_job_meta", "");
        FileUtils.forceDelete(tmpDir); // we need a directory, so delete the file first

        Properties props = config.exportToProperties();
        props.setProperty("kylin.metadata.url", metaDumpUrl);
        // dump metadata
        ResourceStore.dumpResources(config, tmpDir, dumpList, props);

        // copy metadata to target metaUrl
        KylinConfig dstConfig = KylinConfig.createKylinConfig(props);
        MetadataStore.createMetadataStore(dstConfig).uploadFromFile(tmpDir);
        // clean up
        logger.debug("Copied metadata to the target metaUrl, delete the temp dir: {}", tmpDir);
        FileUtils.forceDelete(tmpDir);
    }

    private void deleteJobTmpDirectoryOnExists() {
        StorageURL storageURL = StorageURL.valueOf(getDistMetaUrl());
        String metaPath = storageURL.getParameter("path");

        String[] directories = metaPath.split("/");
        String lastDirectory = directories[directories.length - 1];
        String taskPath = metaPath.substring(0, metaPath.length() - 1 - lastDirectory.length());
        try {
            Path path = new Path(taskPath);
            HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), path);
        } catch (Exception e) {
            logger.error("delete job tmp in path {} failed.", taskPath, e);
        }
    }

    public boolean needMergeMetadata() {
        return false;
    }

    public void mergerMetadata(MetadataMerger merger) {
        throw new UnsupportedOperationException();
    }

}
