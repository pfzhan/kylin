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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
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
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
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
    private static final String DRIVER_OPS = "spark.driver.extraJavaOptions";
    private static final String AM_OPS = "spark.yarn.am.extraJavaOptions";
    private static final String EXECUTOR_OPS = "spark.executor.extraJavaOptions";
    private static final String KRB5CONF_PROPS = "java.security.krb5.conf";
    private static final String HADOOP_CONF_PATH = "./__spark_conf__/__hadoop_conf__/";
    private static final String APP_JAR_NAME = "__app__.jar";
    private static final String EMPTY = "";
    private static final String EQUALS = "=";
    private static final String SPACE = " ";

    protected static final String SPARK_MASTER = "spark.master";
    protected static final String DEPLOY_MODE = "spark.submit.deployMode";
    protected static final String YARN_CLUSTER = "cluster";

    private volatile boolean isYarnCluster = false;

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

    protected void setSparkSubmitClassName(String className) {
        this.setParam(NBatchConstants.P_CLASS_NAME, className);
    }

    public String getSparkSubmitClassName() {
        return this.getParam(NBatchConstants.P_CLASS_NAME);
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
        String argsPath = createArgsFileOnHDFS(config, jobId);
        if (config.isUTEnv()) {
            return runLocalMode(argsPath);
        } else {
            return runSparkSubmit(hadoopConf, jars, kylinJobJar,
                    "-className " + getSparkSubmitClassName() + SPACE + argsPath);
        }
    }

    protected String createArgsFileOnHDFS(KylinConfig config, String jobId) throws ExecuteException {
        val fs = HadoopUtil.getWorkingFileSystem();
        Path path = fs.makeQualified(new Path(config.getJobTmpArgsDir(project, jobId)));
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
        return String.format(Locale.ROOT, "%s.%s.log", config.getJobTmpOutputStorePath(getProject(), getId()),
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

    protected ExecuteResult runSparkSubmit(String hadoopConf, String jars, String kylinJobJar, String appArgs) {
        val patternedLogger = new BufferedLogger(logger);

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

            return ExecuteResult.createSucceed(r.getCmd());
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    protected void killOrphanApplicationIfExists(String jobStepId) {
        try {
            val sparkConf = getSparkConf();
            val sparkMaster = sparkConf.getOrDefault(SPARK_MASTER, "local");
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

    private Map<String, String> getSparkConf() {
        KylinConfig config = getConfig();
        final Map<String, String> sparkConf = getSparkConfigOverride(config);
        // spark.master is sure here.
        this.isYarnCluster = YARN_CLUSTER.equals(sparkConf.get(DEPLOY_MODE));

        // Workaround when there is no underlying file: /etc/krb5.conf
        String krb5Conf = KapConfig.wrap(config).getKerberosKrb5Conf();
        ConfMap confMap = new ConfMap() {
            @Override
            public String get(String key) {
                return sparkConf.get(key);
            }

            @Override
            public void set(String key, String value) {
                sparkConf.put(key, value);
            }
        };
        wrapKrb5Conf(AM_OPS, krb5Conf, confMap);
        wrapKrb5Conf(EXECUTOR_OPS, krb5Conf, confMap);
        wrapExecutorGlobalDictionary(config, sparkConf);

        overrideDriverOps(config, sparkConf);
        return sparkConf;
    }

    private void wrapExecutorGlobalDictionary(KylinConfig config, Map<String, String> sparkConf) {
        StringBuilder sb = new StringBuilder();
        if (sparkConf.containsKey(EXECUTOR_OPS)) {
            sb.append(sparkConf.get(EXECUTOR_OPS));
        }
        sb.append(String.format(Locale.ROOT, " -Dkylin.dictionary.globalV2-store-class-name=%s ",
                config.getGlobalDictV2StoreImpl()));
        sparkConf.put(EXECUTOR_OPS, sb.toString());
    }

    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        Map<String, String> overrides = config.getSparkConfigOverride();
        if (!overrides.containsKey("spark.driver.memory")) {
            overrides.put("spark.driver.memory", computeStepDriverMemory() + "m");
        }

        if (UserGroupInformation.isSecurityEnabled()) {
            overrides.put("spark.hadoop.hive.metastore.sasl.enabled", "true");
        }
        return overrides;
    }

    protected void overrideDriverOps(KylinConfig config, Map<String, String> sparkConfigOverride) {
        StringBuilder sb = new StringBuilder();
        if (sparkConfigOverride.containsKey(DRIVER_OPS)) {
            sb.append(sparkConfigOverride.get(DRIVER_OPS).trim());
        }

        KapConfig kapConfig = KapConfig.wrap(config);

        String serverAddress = config.getServerAddress();

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
        sb.append(String.format(Locale.ROOT, " -Dkylin.hdfs.working.dir=%s ", hdfsWorkingDir));
        sb.append(String.format(Locale.ROOT, " -Dspark.driver.log4j.appender.hdfs.File=%s ", sparkDriverHdfsLogPath));
        wrapLog4jConf(sb, config);
        sb.append(String.format(Locale.ROOT, " -Dspark.driver.rest.server.address=%s ", serverAddress));
        sb.append(String.format(Locale.ROOT, " -Dspark.driver.param.taskId=%s ", getId()));
        sb.append(String.format(Locale.ROOT, " -Dspark.driver.local.logDir=%s ", //
                KapConfig.getKylinLogDirAtBestEffort() + "/spark"));
        sparkConfigOverride.put(DRIVER_OPS, sb.toString().trim());
    }

    private void wrapKrb5Conf(String key, String value, ConfMap confMap) {
        // 'spark.yarn.am.extraJavaOptions', 'krb5.conf'
        String extraOps = confMap.get(key);
        if (Objects.isNull(extraOps)) {
            extraOps = EMPTY;
        }
        if (extraOps.contains(KRB5CONF_PROPS)) {
            return;
        }
        StringBuilder sb = new StringBuilder("-D");
        sb.append(KRB5CONF_PROPS);
        sb.append(EQUALS);
        sb.append(HADOOP_CONF_PATH);
        sb.append(value);
        sb.append(SPACE);
        sb.append(extraOps);
        confMap.set(key, sb.toString());
    }

    protected String generateSparkCmd(String hadoopConf, String jars, String kylinJobJar, String appArgs) {
        StringBuilder sb = new StringBuilder();
        sb.append(
                "export HADOOP_CONF_DIR=%s && %s/bin/spark-submit --class io.kyligence.kap.engine.spark.application.SparkEntry ");

        Map<String, String> sparkConf = getSparkConf();
        for (Map.Entry<String, String> entry : sparkConf.entrySet()) {
            appendSparkConf(sb, entry.getKey(), entry.getValue());
        }
        // extra classpath
        wrapClasspathConf(sb, kylinJobJar);

        // extra jars
        if (sparkConf.containsKey("spark.sql.hive.metastore.jars")
            && !sparkConf.get("spark.sql.hive.metastore.jars").contains("builtin")) {
            jars = jars + COMMA + sparkConf.get("spark.sql.hive.metastore.jars");
        }
        final KylinConfig config = getConfig();
        if (StringUtils.isNotEmpty(config.getExtraJarsPath())) {
            jars = jars + COMMA + config.getExtraJarsPath();
        }

        // Kerberos
        KapConfig kapConfig = KapConfig.wrap(config);
        if (Boolean.TRUE.equals(kapConfig.isKerberosEnabled())) {
            wrapKerberosConf(sb, kapConfig);
        }

        if (isYarnCluster) {
            // http://spark.apache.org/docs/latest/running-on-yarn.html#important-notes
            String driverLog4j = config.getLogSparkDriverPropertiesFile();
            // Caution: an extra white space ended is essential.
            String aliasedLog4j = String.format(Locale.ROOT, "%s#%s ", driverLog4j, //
                    Paths.get(driverLog4j).getFileName().toString());
            sb.append("--files ").append(aliasedLog4j);
            final String aliasedJar = String.format(Locale.ROOT, "%s#%s", kylinJobJar, //
                    Paths.get(kylinJobJar).getFileName().toString());
            // Make sure class SparkDriverHdfsLogAppender will be in NM container's classpath.
            if (StringUtils.isBlank(jars) || jars.equals(kylinJobJar)) {
                jars = aliasedJar;
            } else if (jars.contains(kylinJobJar)) {
                jars = jars.replace(kylinJobJar, aliasedJar);
            } else {
                jars = String.format(Locale.ROOT, "%s,%s", jars, aliasedJar);
            }
        }

        sb.append("--name " + getJobNamePrefix() + "%s ");
        sb.append("--jars %s %s %s ");

        // Three parameters at most per line.
        String cmd = String.format(Locale.ROOT, sb.toString(), hadoopConf, //
                KylinConfigBase.getSparkHome(), getId(), jars, //
                kylinJobJar, appArgs);
        logger.info("spark submit cmd: {}", cmd);
        return cmd;
    }

    private void wrapClasspathConf(StringBuilder sb, String kylinJobJar) {
        final String jobJarName = Paths.get(kylinJobJar).getFileName().toString();
        appendSparkConf(sb, "spark.executor.extraClassPath", jobJarName);
        // In yarn cluster mode, make sure class SparkDriverHdfsLogAppender will be in NM container's classpath.
        appendSparkConf(sb, "spark.driver.extraClassPath", isYarnCluster ? //
                String.format(Locale.ROOT, "%s:%s", APP_JAR_NAME, jobJarName) : kylinJobJar);
    }

    private void wrapLog4jConf(StringBuilder sb, KylinConfig config) {
        // https://issues.apache.org/jira/browse/SPARK-16784
        final String localLog4j = config.getLogSparkDriverPropertiesFile();
        final String log4jName = Paths.get(localLog4j).getFileName().toString();
        if (isYarnCluster || config.getSparkMaster().startsWith("k8s")) {
            // Direct file name.
            sb.append(String.format(Locale.ROOT, " -Dlog4j.configuration=%s ", log4jName));
        } else {
            // Use 'file:' as scheme.
            sb.append(String.format(Locale.ROOT, " -Dlog4j.configuration=file:%s ", localLog4j));
        }
    }

    private void wrapKerberosConf(StringBuilder sb, KapConfig kapConfig) {
        appendSparkConf(sb, "spark.yarn.principal", kapConfig.getKerberosPrincipal());
        appendSparkConf(sb, "spark.yarn.keytab", kapConfig.getKerberosKeytabPath());
        appendSparkConf(sb, "spark.yarn.krb5.conf", kapConfig.getKerberosKrb5ConfPath());
        if (KapConfig.FI_PLATFORM.equals(kapConfig.getKerberosPlatform()) //
                || Boolean.TRUE.equals(kapConfig.getPlatformZKEnable())) {
            appendSparkConf(sb, "spark.yarn.zookeeper.principal", kapConfig.getKerberosZKPrincipal());
            appendSparkConf(sb, "spark.yarn.jaas.conf", kapConfig.getKerberosJaasConfPath());
        }
    }

    protected void appendSparkConf(StringBuilder sb, String key, String value) {
        // Multiple parameters in "--conf" need to be enclosed in single quotes
        sb.append(" --conf '").append(key).append(EQUALS).append(value).append("' ");
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

    protected void attachMetadataAndKylinProps(KylinConfig config, boolean kylinPropsOnly) throws IOException {

        String metaDumpUrl = getDistMetaUrl();
        if (StringUtils.isEmpty(metaDumpUrl)) {
            throw new RuntimeException("Missing metaUrl");
        }

        File tmpDir = File.createTempFile("kylin_job_meta", EMPTY);
        FileUtils.forceDelete(tmpDir); // we need a directory, so delete the file first

        final Properties props = config.exportToProperties();
        removeUnNecessaryDump(props);

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

    private void removeUnNecessaryDump(Properties props) {
        props.remove("kylin.engine.spark-conf.spark.yarn.am.extraJavaOptions");
        props.remove("kylin.engine.spark-conf.spark.executor.extraJavaOptions");

        props.remove("kylin.query.async-query.spark-conf.spark.yarn.am.extraJavaOptions");
        props.remove("kylin.query.async-query.spark-conf.spark.executor.extraJavaOptions");

        props.remove("kylin.storage.columnar.spark-conf.spark.yarn.am.extraJavaOptions");
        props.remove("kylin.storage.columnar.spark-conf.spark.executor.extraJavaOptions");
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

    protected String getJobNamePrefix() {
        return "job_step_";
    }

    public boolean needMergeMetadata() {
        return false;
    }

    public void mergerMetadata(MetadataMerger merger) {
        throw new UnsupportedOperationException();
    }

    private interface ConfMap {
        String get(String key);

        void set(String key, String value);
    }
}
