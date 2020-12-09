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
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.NExecutableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cluster.ClusterManagerFactory;
import io.kyligence.kap.cluster.IClusterManager;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.engine.spark.merger.MetadataMerger;
import io.kyligence.kap.guava20.shaded.common.util.concurrent.UncheckedTimeoutException;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

/**
 *
 */
public class NSparkExecutable extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkExecutable.class);

    private static final String COMMA = ",";

    public String getDataflowId() {
        return this.getParam(NBatchConstants.P_DATAFLOW_ID);
    }

    @Override
    public Set<String> getSegmentIds() {
        return Sets.newHashSet(StringUtils.split(this.getParam(NBatchConstants.P_SEGMENT_IDS), COMMA));
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

    private boolean isLocalFs() {
        String fs = HadoopUtil.getWorkingFileSystem().getUri().toString();
        return fs.startsWith("file:");
    }

    private String getDistMetaFs() {
        String defaultFs = HadoopUtil.getWorkingFileSystem().getUri().toString();
        String engineWriteFs = KylinConfig.getInstanceFromEnv().getEngineWriteFs();
        return StringUtils.isBlank(engineWriteFs) ? defaultFs : engineWriteFs;
    }

    protected void setDistMetaUrl(StorageURL storageURL) {
        String fs = getDistMetaFs();
        HashMap<String, String> stringStringHashMap = Maps.newHashMap(storageURL.getAllParameters());
        if (!isLocalFs()) {
            stringStringHashMap.put("path", fs + storageURL.getParameter("path"));
        }
        StorageURL copy = storageURL.copy(stringStringHashMap);
        this.setParam(NBatchConstants.P_DIST_META_URL, copy.toString());
        this.setParam(NBatchConstants.P_OUTPUT_META_URL, copy + "_output");
    }

    public String getDistMetaUrl() {
        return this.getParam(NBatchConstants.P_DIST_META_URL);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        this.setLogPath(getSparkDriverLogHdfsPath(context.getConfig()));
        final KylinConfig config = getConfig();

        String sparkHome = KylinConfigBase.getSparkHome();
        if (StringUtils.isEmpty(sparkHome) && !config.isUTEnv()) {
            throw new RuntimeException("Missing spark home");
        }

        String kylinJobJar = config.getKylinJobJarPath();
        if (StringUtils.isEmpty(kylinJobJar) && !config.isUTEnv()) {
            throw new RuntimeException("Missing kylin job jar");
        }
        String hadoopConf = HadoopUtil.getHadoopConfDir();

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

        onExecuteStart();

        try {
            // if building job is resumable,
            // property value contains placeholder (eg. "kylin.engine.spark-conf.spark.yarn.dist.files") will be replaced with specified path.
            // in case of ha, not every candidate node will have the same path
            // upload kylin.properties only
            attachMetadataAndKylinProps(config, isResumable());
        } catch (IOException e) {
            throw new ExecuteException("meta dump failed", e);
        }

        if (!isResumable()) {
            // set resumable when metadata and props attached
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).setJobResumable(getId());
                return 0;
            }, context.getEpochId(), project);
        }

        String jobId = getId();
        String filePath = createArgsFileOnHDFS(config, jobId);
        if (config.isUTEnv()) {
            return runLocalMode(filePath);
        } else {
            return runSparkSubmit(hadoopConf, jars, kylinJobJar,
                    "-className " + getSparkSubmitClassName() + " " + filePath);
        }
    }

    protected String createArgsFileOnHDFS(KylinConfig config, String jobId) throws ExecuteException {
        Path path = new Path(config.getJobTmpArgsDir(project, jobId));
        val fs = HadoopUtil.getWorkingFileSystem();
        try (FSDataOutputStream out = fs.create(path)) {
            val params = filterEmptySegments(getParams());

            out.write(JsonUtil.writeValueAsBytes(params));
        } catch (IOException e) {
            try {
                fs.delete(path, true);
            } catch (IOException e1) {
                throw new ExecuteException("Write spark args failed! Error for delete file: " + path.toString(), e1);
            }
            throw new ExecuteException("Write spark args failed: ", e);
        }
        return path.toString();
    }

    /**
     * segments may have been deleted after job created
     * @param originParams
     * @return
     */
    @VisibleForTesting
    Map<String, String> filterEmptySegments(final Map<String, String> originParams) {
        Map<String, String> copied = Maps.newHashMap(originParams);
        String originSegments = copied.get(NBatchConstants.P_SEGMENT_IDS);
        String dfId = getDataflowId();
        final NDataflow dataFlow = NDataflowManager.getInstance(getConfig(), getProject()).getDataflow(dfId);
        if (Objects.isNull(dataFlow) || StringUtils.isBlank(originSegments)) {
            return copied;
        }
        String newSegments = Stream.of(StringUtils.split(originSegments, COMMA))
                .filter(id -> Objects.nonNull(dataFlow.getSegment(id))).collect(Collectors.joining(COMMA));
        copied.put(NBatchConstants.P_SEGMENT_IDS, newSegments);
        return copied;
    }

    /**
     * generate the spark driver log hdfs path format, json path + timestamp + .log
     *
     * @param config
     * @return
     */
    public String getSparkDriverLogHdfsPath(KylinConfig config) {
        return String.format("%s.%s.log", config.getJobTmpOutputStorePath(getProject(), getId()),
                System.currentTimeMillis());
    }

    @Override
    protected KylinConfig getConfig() {
        val originalConfig = KylinConfig.getInstanceFromEnv();
        KylinConfigExt kylinConfigExt = null;
        val project = getProject();
        Preconditions.checkState(StringUtils.isNotBlank(project), "job " + getId() + " project info is empty");
        val dataflow = getParam(NBatchConstants.P_DATAFLOW_ID);
        if (StringUtils.isNotBlank(dataflow)) {
            val dataflowManager = NDataflowManager.getInstance(originalConfig, project);
            kylinConfigExt = dataflowManager.getDataflow(dataflow).getConfig();
        } else {
            val projectInstance = NProjectManager.getInstance(originalConfig).getProject(project);
            kylinConfigExt = projectInstance.getConfig();
        }

        val jobOverrides = Maps.<String, String> newHashMap();
        val parentId = getParentId();
        jobOverrides.put("job.id", StringUtils.defaultIfBlank(parentId, getId()));
        jobOverrides.put("job.project", project);
        if (StringUtils.isNotBlank(originalConfig.getMountSparkLogDir())) {
            jobOverrides.put("job.mountDir", originalConfig.getMountSparkLogDir());
        }
        if (StringUtils.isNotBlank(parentId)) {
            jobOverrides.put("job.stepId", getId());
        }
        jobOverrides.put("user.timezone", KylinConfig.getInstanceFromEnv().getTimeZone());
        jobOverrides.put("spark.driver.log4j.appender.hdfs.File",
                Objects.isNull(this.getLogPath()) ? "null" : this.getLogPath());
        jobOverrides.putAll(kylinConfigExt.getExtendedOverrides());

        return KylinConfigExt.createInstance(kylinConfigExt, jobOverrides);
    }

    private ExecuteResult runSparkSubmit(String hadoopConf, String jars, String kylinJobJar, String appArgs) {
        val config = getConfig();
        PatternedLogger patternedLogger;
        if (config.isJobLogPrintEnabled()) {
            patternedLogger = new PatternedLogger(logger);
        } else {
            patternedLogger = new PatternedLogger(null);
        }

        try {
            killOrphanApplicationIfExists(getId());
            String cmd = generateSparkCmd(hadoopConf, jars, kylinJobJar, appArgs);

            CliCommandExecutor exec = new CliCommandExecutor();
            CliCommandExecutor.CliCmdExecResult r = exec.execute(cmd, patternedLogger, getParentId());
            if (StringUtils.isNotEmpty(r.getProcessId())) {
                try {
                    Map<String, String> updateInfo = Maps.newHashMap();
                    updateInfo.put("process_id", r.getProcessId());
                    EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                                .updateJobOutput(getParentId(), this.getStatus(), updateInfo, null, null);
                        return null;
                    }, project);
                } catch (Exception e) {
                    logger.warn("failed to record process id.");
                }
            }

            Map<String, String> extraInfo = makeExtraInfo(patternedLogger.getInfo());
            val ret = ExecuteResult.createSucceed(r.getCmd());
            ret.getExtraInfo().putAll(extraInfo);
            return ret;
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    private void killOrphanApplicationIfExists(String jobStepId) {
        try {
            val sparkConfs = getSparkConfigOverride();
            val sparkMaster = sparkConfs.getOrDefault("spark.master", "local");
            if (sparkMaster.startsWith("local")) {
                logger.info("Skip kill orphan app for spark.master={}", sparkMaster);
                return;
            }
            final IClusterManager cm = ClusterManagerFactory.create(getConfig());
            cm.killApplication(jobStepId);
        } catch (UncheckedTimeoutException e) {
            logger.warn("Kill orphan app timeout {}", e.getMessage());
        }
    }

    protected Map<String, String> getSparkConfigOverride() {
        val config = getConfig();
        Map<String, String> sparkConfigOverride = config.getSparkConfigOverride();
        if (!sparkConfigOverride.containsKey("spark.driver.memory")) {
            sparkConfigOverride.put("spark.driver.memory", computeStepDriverMemory() + "m");
        }

        if (UserGroupInformation.isSecurityEnabled()) {
            sparkConfigOverride.put("spark.hadoop.hive.metastore.sasl.enabled", "true");
        }
        replaceSparkDriverJavaOpsConfIfNeeded(config, sparkConfigOverride);
        return sparkConfigOverride;
    }

    private void replaceSparkDriverJavaOpsConfIfNeeded(KylinConfig config, Map<String, String> sparkConfigOverride) {
        String sparkDriverExtraJavaOptionsKey = "spark.driver.extraJavaOptions";
        StringBuilder sb = new StringBuilder();
        if (sparkConfigOverride.containsKey(sparkDriverExtraJavaOptionsKey)) {
            sb.append(sparkConfigOverride.get(sparkDriverExtraJavaOptionsKey));
        }

        KapConfig kapConfig = KapConfig.wrap(config);

        String serverIp = config.getServerName();
        String serverPort = config.getServerPort();

        String hdfsWorkingDir = config.getHdfsWorkingDirectory();

        String sparkDriverHdfsLogPath = null;
        if (config instanceof KylinConfigExt) {
            Map<String, String> extendedOverrides = ((KylinConfigExt) config).getExtendedOverrides();
            if (Objects.nonNull(extendedOverrides)) {
                sparkDriverHdfsLogPath = extendedOverrides.get("spark.driver.log4j.appender.hdfs.File");
            }
        }
        if (kapConfig.isCloud()) {
            String logLocalWorkingDirectory = config.getLogLocalWorkingDirectory();
            if (StringUtils.isNotBlank(logLocalWorkingDirectory)) {
                hdfsWorkingDir = logLocalWorkingDirectory;
                sparkDriverHdfsLogPath = logLocalWorkingDirectory + sparkDriverHdfsLogPath;
            }
        }

        String log4jConfiguration = "file:" + config.getLogSparkDriverPropertiesFile();
        sb.append(String.format(" -Dlog4j.configuration=%s ", log4jConfiguration));
        sb.append(String.format(" -Dkylin.kerberos.enabled=%s ", kapConfig.isKerberosEnabled()));

        if (kapConfig.isKerberosEnabled()) {
            sb.append(String.format(" -Dkylin.kerberos.principal=%s ", kapConfig.getKerberosPrincipal()));
            sb.append(String.format(" -Dkylin.kerberos.keytab=%s", kapConfig.getKerberosKeytabPath()));
            if (kapConfig.getKerberosPlatform().equalsIgnoreCase(KapConfig.FI_PLATFORM)
                    || kapConfig.getPlatformZKEnable()) {
                sb.append(String.format(" -Djava.security.auth.login.config=%s", kapConfig.getKerberosJaasConfPath()));
                sb.append(String.format(" -Djava.security.krb5.conf=%s", kapConfig.getKerberosKrb5ConfPath()));
            }
        }
        sb.append(String.format(" -Dkylin.hdfs.working.dir=%s ", hdfsWorkingDir));
        sb.append(String.format(" -Dspark.driver.log4j.appender.hdfs.File=%s ", sparkDriverHdfsLogPath));
        sb.append(String.format(" -Dspark.driver.rest.server.ip=%s ", serverIp));
        sb.append(String.format(" -Dspark.driver.rest.server.port=%s ", serverPort));
        sb.append(String.format(" -Dspark.driver.param.taskId=%s ", getId()));
        sb.append(String.format(" -Dspark.driver.local.logDir=%s ", KapConfig.getKylinLogDirAtBestEffort() + "/spark"));
        sparkConfigOverride.put(sparkDriverExtraJavaOptionsKey, sb.toString());
    }

    protected String generateSparkCmd(String hadoopConf, String jars, String kylinJobJar, String appArgs) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "export HADOOP_CONF_DIR=%s && %s/bin/spark-submit --class io.kyligence.kap.engine.spark.application.SparkEntry ");

        Map<String, String> sparkConfs = getSparkConfigOverride();
        for (Entry<String, String> entry : sparkConfs.entrySet()) {
            appendSparkConf(sb, entry.getKey(), entry.getValue());
        }
        appendSparkConf(sb, "spark.executor.extraClassPath", Paths.get(kylinJobJar).getFileName().toString());
        appendSparkConf(sb, "spark.driver.extraClassPath", kylinJobJar);

        if (sparkConfs.containsKey("spark.sql.hive.metastore.jars")) {
            jars = jars + COMMA + sparkConfs.get("spark.sql.hive.metastore.jars");
        }

        val config = getConfig();
        if (!Strings.isEmpty(config.getExtraJarsPath())) {
            jars = jars + COMMA + config.getExtraJarsPath();
        }

        sb.append("--name job_step_%s ");
        sb.append("--jars %s %s %s");
        String cmd = String.format(sb.toString(), hadoopConf, KylinConfigBase.getSparkHome(), getId(), jars,
                kylinJobJar, appArgs);
        logger.info("spark submit cmd: {}", cmd);
        return cmd;
    }

    protected void appendSparkConf(StringBuilder sb, String key, String value) {
        // Multiple parameters in "--conf" need to be enclosed in single quotes
        sb.append(" --conf '").append(key).append("=").append(value).append("' ");
    }

    private ExecuteResult runLocalMode(String appArgs) {
        try {
            Class<?> appClz = ClassUtil.forName(getSparkSubmitClassName(), Object.class);
            appClz.getMethod("main", String[].class).invoke(appClz.newInstance(), (Object) new String[] { appArgs });
            return ExecuteResult.createSucceed();
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    protected Set<String> getMetadataDumpList(KylinConfig config) {
        return Collections.emptySet();
    }

    void attachMetadataAndKylinProps(KylinConfig config) throws IOException {
        attachMetadataAndKylinProps(config, false);
    }

    private void attachMetadataAndKylinProps(KylinConfig config, boolean kylinPropsOnly) throws IOException {

        String metaDumpUrl = getDistMetaUrl();
        if (StringUtils.isEmpty(metaDumpUrl)) {
            throw new RuntimeException("Missing metaUrl");
        }

        File tmpDir = File.createTempFile("kylin_job_meta", "");
        FileUtils.forceDelete(tmpDir); // we need a directory, so delete the file first

        Properties props = config.exportToProperties();
        props.setProperty("kylin.metadata.url", metaDumpUrl);

        if (kylinPropsOnly) {
            ResourceStore.dumpKylinProps(tmpDir, props);
        } else {
            // The way of Updating metadata is CopyOnWrite. So it is safe to use Reference in the value.
            Map<String, RawResource> dumpMap = EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(
                    UnitOfWorkParams.<Map> builder().readonly(true).unitName(getProject()).processor(() -> {
                        Map<String, RawResource> retMap = Maps.newHashMap();
                        for (String resPath : getMetadataDumpList(config)) {
                            ResourceStore resourceStore = ResourceStore.getKylinMetaStore(config);
                            RawResource rawResource = resourceStore.getResource(resPath);
                            retMap.put(resPath, rawResource);
                        }
                        return retMap;
                    }).build());

            if (Objects.isNull(dumpMap) || dumpMap.isEmpty()) {
                return;
            }
            // dump metadata
            ResourceStore.dumpResourceMaps(config, tmpDir, dumpMap, props);
        }

        // copy metadata to target metaUrl
        KylinConfig dstConfig = KylinConfig.createKylinConfig(props);
        MetadataStore.createMetadataStore(dstConfig).uploadFromFile(tmpDir);
        // clean up
        logger.debug("Copied metadata to the target metaUrl, delete the temp dir: {}", tmpDir);
        FileUtils.forceDelete(tmpDir);
    }

    private void deleteJobTmpDirectoryOnExists() {
        if (!getConfig().isDeleteJobTmpWhenRetry()) {
            return;
        }
        if (isResumable()) {
            return;
        }
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
