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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
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
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.JobStoppedException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ChainedStageExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.execution.StageBase;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.engine.spark.merger.MetadataMerger;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

/**
 *
 */
public class NSparkExecutable extends AbstractExecutable implements ChainedStageExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkExecutable.class);

    private static final String AM_EXTRA_JAVA_OPTIONS = "spark.yarn.am.extraJavaOptions";
    private static final String DRIVER_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions";
    private static final String EXECUTOR_EXTRA_JAVA_OPTIONS = "spark.executor.extraJavaOptions";
    private static final String HADOOP_CONF_PATH = "./__spark_conf__/__hadoop_conf__/";
    private static final String APP_JAR_NAME = "__app__.jar";

    private static final String SPARK_JARS_1 = "spark.jars";
    private static final String SPARK_JARS_2 = "spark.yarn.dist.jars";
    private static final String SPARK_FILES_1 = "spark.files";
    private static final String SPARK_FILES_2 = "spark.yarn.dist.files";

    private static final String COMMA = ",";
    private static final String COLON = ":";
    private static final String EMPTY = "";
    private static final String SPACE = " ";
    private static final String SUBMIT_LINE_FORMAT = " \\\n";

    private static final String DRIVER_EXTRA_CLASSPATH = "spark.driver.extraClassPath";
    private static final String EXECUTOR_EXTRA_CLASSPATH = "spark.executor.extraClassPath";

    protected static final String SPARK_MASTER = "spark.master";
    protected static final String DEPLOY_MODE = "spark.submit.deployMode";
    protected static final String CLUSTER_MODE = "cluster";
    protected ISparkJobHandler sparkJobHandler;

    private transient final List<StageBase> stages = Lists.newCopyOnWriteArrayList();
    private final Map<String, List<StageBase>> stagesMap = Maps.newConcurrentMap();

    public NSparkExecutable() {
        super();
        logger.info("Has-args NSparkExecutable");
        initHandler();
    }

    public NSparkExecutable(Object notSetId) {
        super(notSetId);
        logger.info("No-args NSparkExecutable");
        initHandler();
    }

    public String getDataflowId() {
        return this.getParam(NBatchConstants.P_DATAFLOW_ID);
    }

    protected void initHandler() {
        logger.info("Handler class name:" + KylinConfig.getInstanceFromEnv().getSparkBuildJobHandlerClassName());
        sparkJobHandler = (ISparkJobHandler) ClassUtil
                .newInstance(KylinConfig.getInstanceFromEnv().getSparkBuildJobHandlerClassName());
    }

    @Override
    public Set<String> getSegmentIds() {
        return Sets.newHashSet(StringUtils.split(this.getParam(NBatchConstants.P_SEGMENT_IDS), COMMA));
    }

    public Set<Long> getCuboidLayoutIds() {
        return NSparkCubingUtil.str2Longs(this.getParam(NBatchConstants.P_LAYOUT_IDS));
    }

    protected void setSparkSubmitClassName(String className) {
        if (KylinConfig.getInstanceFromEnv().getSparkEngineBuildStepsToSkip().contains(this.getClass().getName())) {
            className = EmptyPlaceholderJob.class.getName();
        }
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
        String result = StringUtils.isBlank(engineWriteFs) ? defaultFs : engineWriteFs;
        if (result.startsWith(HadoopUtil.MAPR_FS_PREFIX)) {
            return HadoopUtil.MAPR_FS_PREFIX;
        } else {
            return result;
        }
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

    public void waiteForResourceStart(ExecutableContext context) {
        // mark waiteForResource stage start
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getExecutableManager(getProject()) //
                    .updateStageStatus(getId() + "_00", null, ExecutableState.RUNNING, null, null);
            return 0;
        }, context.getEpochId(), project);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        waiteForResourceStart(context);

        this.setLogPath(getSparkDriverLogHdfsPath(context.getConfig()));
        final KylinConfig config = getConfig();

        String jobId = getId();
        if (!config.isDevOrUT()) {
            setDistMetaUrl(config.getJobTmpMetaStoreUrl(project, jobId));
        }

        String sparkHome = KylinConfigBase.getSparkHome();
        if (StringUtils.isEmpty(sparkHome) && !config.isUTEnv()) {
            throw new RuntimeException("Missing spark home");
        }

        String kylinJobJar = config.getKylinJobJarPath();
        if (StringUtils.isEmpty(kylinJobJar) && !config.isUTEnv()) {
            throw new RuntimeException("Missing kylin job jar");
        }
        sparkJobHandler.checkApplicationJar(config, kylinJobJar);

        String hadoopConfDir = HadoopUtil.getHadoopConfDir();

        File hiveConfFile = new File(hadoopConfDir, "hive-site.xml");
        if (!hiveConfFile.exists() && !config.isUTEnv()) {
            throw new RuntimeException("Cannot find hive-site.xml in kylin_hadoop_conf_dir: " + hadoopConfDir + //
                    ". In order to enable spark cubing, you must set kylin.env.hadoop-conf-dir to a dir which contains at least core-site.xml, hdfs-site.xml, hive-site.xml, mapred-site.xml, yarn-site.xml");
        }
        deleteSnapshotDirectoryOnExists();
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

        sparkJobHandler.prepareEnviroment(project, jobId, getParams());

        String argsPath = createArgsFileOnHDFS(config, jobId);
        if (config.isUTEnv()) {
            return runLocalMode(argsPath);
        } else {
            return runSparkSubmit(hadoopConfDir, kylinJobJar,
                    "-className " + getSparkSubmitClassName() + SPACE + argsPath);
        }
    }

    @Override
    protected void onExecuteStart() throws JobStoppedException {
        wrapWithCheckQuit(() -> {
            final Map<String, String> sparkConf = getSparkConf();
            Map<String, String> jobParams = Maps.newHashMap();
            jobParams.put("job_params", JsonUtil.writeValueAsString(sparkConf));
            updateJobOutput(project, getId(), ExecutableState.RUNNING, jobParams, null, null);
        });
    }

    protected String createArgsFileOnHDFS(KylinConfig config, String jobId) throws ExecuteException {
        return sparkJobHandler.createArgsFileOnRemoteFileSystem(config, getProject(), jobId, this.getParams());
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
        removeFactTableInExcludedTables(dataFlow, copied);
        String newSegments = Stream.of(StringUtils.split(originSegments, COMMA))
                .filter(id -> Objects.nonNull(dataFlow.getSegment(id))).collect(Collectors.joining(COMMA));
        copied.put(NBatchConstants.P_SEGMENT_IDS, newSegments);
        return copied;
    }

    private void removeFactTableInExcludedTables(NDataflow dataFlow, final Map<String, String> originParams) {
        val rootFactTableName = dataFlow.getModel().getRootFactTableName();
        val excludedTablesString = originParams.getOrDefault(NBatchConstants.P_EXCLUDED_TABLES, "");
        if (StringUtils.isBlank(excludedTablesString)) {
            return;
        }
        val excludedTables = Sets.newHashSet(excludedTablesString.split(","));
        excludedTables.remove(rootFactTableName);
        originParams.put(NBatchConstants.P_EXCLUDED_TABLES, String.join(",", excludedTables));
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

    private Boolean checkHadoopWorkingDir() {
        // read hdfs.working.dir in kylin config
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final String hdfsWorkingDirectory = kylinConfig.getHdfsWorkingDirectory();
        // read hdfs.working.dir
        final Properties properties = KylinConfig.buildSiteProperties();
        final String hdfsWorkingDirectoryFromProperties = kylinConfig.getHdfsWorkingDirectoryFromProperties(properties);
        return StringUtils.equals(hdfsWorkingDirectory, hdfsWorkingDirectoryFromProperties);
    }

    @Override
    protected KylinConfig getConfig() {
        val originalConfig = KylinConfig.getInstanceFromEnv();
        if (!originalConfig.isDevOrUT() && !checkHadoopWorkingDir()) {
            KylinConfig.getInstanceFromEnv().reloadKylinConfigPropertiesFromSiteProperties();
        }
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

        if (getParent() != null) {
            String yarnQueue = getParent().getSparkYarnQueue();
            // TODO double check if valid yarn queue
            if (!Strings.isNullOrEmpty(yarnQueue)) {
                jobOverrides.put("kylin.engine.spark-conf." + SPARK_YARN_QUEUE, yarnQueue);
            }
        }
        return KylinConfigExt.createInstance(kylinConfigExt, jobOverrides);
    }

    public SparkAppDescription getSparkAppDesc() {
        val desc = new SparkAppDescription();

        val conf = getConfig();
        desc.setJobNamePrefix(getJobNamePrefix());
        desc.setProject(getProject());
        desc.setJobId(getId());
        desc.setStepId(getStepId());
        desc.setSparkSubmitClassName(getSparkSubmitClassName());

        val sparkConf = getSparkConf(conf);
        desc.setSparkConf(sparkConf);
        desc.setCOMMA(COMMA);
        desc.setSparkJars(getSparkJars(conf, sparkConf));
        desc.setSparkFiles(getSparkFiles(conf, sparkConf));
        return desc;
    }

    protected ExecuteResult runSparkSubmit(String hadoopConfDir, String kylinJobJar, String appArgs) {
        val patternedLogger = new BufferedLogger(logger);
        killOrphanApplicationIfExists(getId());
        try {
            Object cmd;
            val desc = getSparkAppDesc();
            desc.setHadoopConfDir(hadoopConfDir);
            desc.setKylinJobJar(kylinJobJar);
            desc.setAppArgs(appArgs);

            cmd = sparkJobHandler.generateSparkCmd(getConfig(), desc);

            String output = sparkJobHandler.runSparkSubmit(cmd, patternedLogger, getParentId(), getProject(),
                    getStatus());

            return ExecuteResult.createSucceed(output);
        } catch (Exception e) {
            wrapWithExecuteExceptionUpdateJobError(e);
            return ExecuteResult.createError(e);
        }
    }

    public void killOrphanApplicationIfExists(String jobStepId) {
        sparkJobHandler.killOrphanApplicationIfExists(jobStepId, getConfig(), getSparkConf());
    }

    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        Map<String, String> confMap = config.getSparkConfigOverride();
        final String driverMemConf = "spark.driver.memory";
        if (!confMap.containsKey(driverMemConf)) {
            confMap.put(driverMemConf, computeStepDriverMemory() + "m");
        }

        if (UserGroupInformation.isSecurityEnabled()) {
            confMap.put("spark.hadoop.hive.metastore.sasl.enabled", "true");
        }
        return confMap;
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
        // If we don't remove these configurations,
        // they will be overwritten in the SparkApplication
        props.setProperty("kylin.metadata.url", metaDumpUrl);
        modifyDump(props);

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
        Properties propsForMetaStore = config.exportToProperties();
        propsForMetaStore.setProperty("kylin.metadata.url", metaDumpUrl);
        KylinConfig dstConfig = KylinConfig.createKylinConfig(propsForMetaStore);
        MetadataStore.createMetadataStore(dstConfig).uploadFromFile(tmpDir);
        // clean up
        logger.debug("Copied metadata to the target metaUrl, delete the temp dir: {}", tmpDir);
        FileUtils.forceDelete(tmpDir);
    }

    private void modifyDump(Properties props) {
        sparkJobHandler.modifyDump(props);
        removeUnNecessaryDump(props);
    }

    private void removeUnNecessaryDump(Properties props) {
        // Rewrited thru '--jars'.
        props.remove("kylin.engine.spark-conf.spark.jars");
        props.remove("kylin.engine.spark-conf.spark.yarn.dist.jars");
        // Rewrited thru '--files'.
        props.remove("kylin.engine.spark-conf.spark.files");
        props.remove("kylin.engine.spark-conf.spark.yarn.dist.files");

        // Rewrited.
        props.remove("kylin.engine.spark-conf.spark.driver.extraJavaOptions");
        props.remove("kylin.engine.spark-conf.spark.yarn.am.extraJavaOptions");
        props.remove("kylin.engine.spark-conf.spark.executor.extraJavaOptions");

        // Rewrited.
        props.remove("kylin.engine.spark-conf.spark.driver.extraClassPath");
        props.remove("kylin.engine.spark-conf.spark.executor.extraClassPath");

        props.remove("kylin.query.async-query.spark-conf.spark.yarn.am.extraJavaOptions");
        props.remove("kylin.query.async-query.spark-conf.spark.executor.extraJavaOptions");

        props.remove("kylin.storage.columnar.spark-conf.spark.yarn.am.extraJavaOptions");
        props.remove("kylin.storage.columnar.spark-conf.spark.executor.extraJavaOptions");
    }

    private void deleteSnapshotDirectoryOnExists() {
        if (isResumable()) {
            return;
        }
        KylinConfig kylinConf = KylinConfig.getInstanceFromEnv();
        String snapshotPath = kylinConf.getSnapshotCheckPointDir(getProject(), getId().split("_")[0]);
        try {
            Path path = new Path(snapshotPath);
            HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), path);
        } catch (Exception e) {
            logger.error("delete snapshot checkpoint in path {} failed.", snapshotPath, e);
        }
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

    protected String getExtJar() {
        return EMPTY;
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

    @Override
    public AbstractExecutable addStage(AbstractExecutable step) {
        int stepId = stages.size();

        step.setId(getId() + "_" + String.format(Locale.ROOT, "%02d", stepId));
        step.setParent(this);
        step.setStepId(stepId);
        this.stages.add(((StageBase) step));
        return step;
    }

    @Override
    public void setStageMap() {
        if (CollectionUtils.isEmpty(stages)) {
            return;
        }
        // when table sampling and snapshot build, null segmentIds, use jobId
        if (StringUtils.isBlank(getParam(NBatchConstants.P_SEGMENT_IDS))) {
            stagesMap.put(getId(), stages);
            return;
        }
        for (String segmentId : getSegmentIds()) {
            stagesMap.put(segmentId, stages);
        }
        // when layout ids not null, set index count
        if (StringUtils.isNotBlank(getParam(NBatchConstants.P_LAYOUT_IDS))) {
            int indexCount = StringUtil.splitAndTrim(getParam(NBatchConstants.P_LAYOUT_IDS), ",").length;
            setParam(NBatchConstants.P_INDEX_COUNT, String.valueOf(indexCount));
        }
    }

    @Override
    public void setStageMapWithSegment(String id, List<StageBase> steps) {
        final List<StageBase> old = stagesMap.getOrDefault(id, Lists.newCopyOnWriteArrayList());
        old.addAll(steps);
        stagesMap.put(id, steps);
    }

    @Override
    public Map<String, List<StageBase>> getStagesMap() {
        return stagesMap;
    }

    private boolean isClusterMode(Map<String, String> sparkConf) {
        return CLUSTER_MODE.equals(sparkConf.get(DEPLOY_MODE));
    }

    private Map<String, String> getSparkConf() {
        return getSparkConf(getConfig());
    }

    private Map<String, String> getSparkConf(KylinConfig kylinConf) {

        KapConfig kapConf = KapConfig.wrap(kylinConf);

        //以下4方法全部重写，可以基本解决问题
        //TODO 重写，在这里就把serverless用不着的东西去不抛掉
        Map<String, String> sparkConf = getSparkConfigOverride(kylinConf);

        // Rewrite kerberos conf.
        rewriteKerberosConf(kapConf, sparkConf);

        //TODO 重写
        // Rewrite driver extra java options.
        rewriteDriverExtraJavaOptions(kylinConf, kapConf, sparkConf);

        //TODO 重写
        // Rewrite executor extra java options.
        rewriteExecutorExtraJavaOptions(kylinConf, sparkConf);

        // Rewrite extra classpath.
        rewriteExtraClasspath(kylinConf, sparkConf);

        return Collections.unmodifiableMap(sparkConf);
    }

    private void rewriteDriverExtraJavaOptions(KylinConfig kylinConf, KapConfig kapConf, //
            Map<String, String> sparkConf) {
        StringBuilder sb = new StringBuilder();
        if (sparkConf.containsKey(DRIVER_EXTRA_JAVA_OPTIONS)) {
            sb.append(sparkConf.get(DRIVER_EXTRA_JAVA_OPTIONS));
        }

        String hdfsWorkingDir = kylinConf.getHdfsWorkingDirectory();
        String sparkDriverHdfsLogPath = null;
        if (kylinConf instanceof KylinConfigExt) {
            Map<String, String> extendedOverrides = ((KylinConfigExt) kylinConf).getExtendedOverrides();
            if (Objects.nonNull(extendedOverrides)) {
                sparkDriverHdfsLogPath = extendedOverrides.get("spark.driver.log4j.appender.hdfs.File");
            }
        }
        if (kapConf.isCloud()) {
            String logLocalWorkingDirectory = kylinConf.getLogLocalWorkingDirectory();
            if (StringUtils.isNotBlank(logLocalWorkingDirectory)) {
                hdfsWorkingDir = logLocalWorkingDirectory;
                sparkDriverHdfsLogPath = logLocalWorkingDirectory + sparkDriverHdfsLogPath;
            }
        }
        sb.append(SPACE).append("-Dkylin.hdfs.working.dir=").append(hdfsWorkingDir);
        sb.append(SPACE).append("-Dspark.driver.log4j.appender.hdfs.File=").append(sparkDriverHdfsLogPath);

        // Log4j conf.
        rewriteDriverLog4jConf(sb, kylinConf, sparkConf);

        sb.append(SPACE).append("-Dspark.driver.rest.server.address=").append(kylinConf.getServerAddress());
        sb.append(SPACE).append("-Dspark.driver.param.taskId=").append(getId());
        sb.append(SPACE).append("-Dspark.driver.local.logDir=").append(KapConfig.getKylinLogDirAtBestEffort()) //
                .append("/spark");

        if (kapConf.getPlatformZKEnable()) {
            sb.append(SPACE).append("-Djava.security.auth.login.config=").append(kapConf.getKerberosJaasConfPath());
        }

        sparkConf.put(DRIVER_EXTRA_JAVA_OPTIONS, sb.toString().trim());
    }

    @VisibleForTesting
    public String getDriverExtraJavaOptions(KylinConfig kylinConf) {
        KapConfig kapConf = KapConfig.wrap(kylinConf);
        Map<String, String> sparkConf = getSparkConfigOverride(kylinConf);
        rewriteDriverExtraJavaOptions(kylinConf, kapConf, sparkConf);
        return sparkConf.get(DRIVER_EXTRA_JAVA_OPTIONS);
    }

    //TODO是不是也写回去比较好
    private void rewriteKerberosConf(KapConfig kapConf, final Map<String, String> sparkConf) {
        if (Boolean.FALSE.equals(kapConf.isKerberosEnabled())) {
            return;
        }
        // Yarn client will upload the related file automatically.
        // We wouldn't put the file on --files.
        sparkConf.put("spark.kerberos.principal", kapConf.getKerberosPrincipal());
        sparkConf.put("spark.kerberos.keytab", kapConf.getKerberosKeytabPath());

        // Workaround when there is no underlying file: /etc/krb5.conf
        String remoteKrb5 = HADOOP_CONF_PATH + kapConf.getKerberosKrb5Conf();
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
        // There are conventions here:
        // a) krb5.conf is underlying ${KYLIN_HOME}/conf/
        // b) krb5.conf is underlying ${KYLIN_HOME}/hadoop_conf/
        // Wrap driver ops krb5.conf depends on deploy mode
        if (isClusterMode(sparkConf)) {
            // remote for 'yarn cluster'
            rewriteSpecifiedKrb5Conf(DRIVER_EXTRA_JAVA_OPTIONS, remoteKrb5, confMap);
        } else {
            // local for 'yarn client' & 'spark local'
            rewriteSpecifiedKrb5Conf(DRIVER_EXTRA_JAVA_OPTIONS, kapConf.getKerberosKrb5ConfPath(), confMap);
        }
        rewriteSpecifiedKrb5Conf(AM_EXTRA_JAVA_OPTIONS, remoteKrb5, confMap);
        rewriteSpecifiedKrb5Conf(EXECUTOR_EXTRA_JAVA_OPTIONS, remoteKrb5, confMap);
    }

    //TODO 可重写，serverless中
    //不需要这3个， -Dlog4j.configuration,-Dkap.spark.mountDir,-Dorg.xerial.snappy.tempdir
    private void rewriteExecutorExtraJavaOptions(KylinConfig kylinConf, Map<String, String> sparkConf) {
        StringBuilder sb = new StringBuilder();
        if (sparkConf.containsKey(EXECUTOR_EXTRA_JAVA_OPTIONS)) {
            sb.append(sparkConf.get(EXECUTOR_EXTRA_JAVA_OPTIONS));
        }
        sb.append(SPACE).append("-Dkylin.dictionary.globalV2-store-class-name=") //
                .append(kylinConf.getGlobalDictV2StoreImpl());
        sparkConf.put(EXECUTOR_EXTRA_JAVA_OPTIONS, sb.toString().trim());
    }

    //TODO serverless不需要它
    private void rewriteSpecifiedKrb5Conf(String key, String value, ConfMap confMap) {
        String originOptions = confMap.get(key);
        if (Objects.isNull(originOptions)) {
            originOptions = EMPTY;
        }
        if (originOptions.contains("-Djava.security.krb5.conf")) {
            return;
        }
        String newOptions = "-Djava.security.krb5.conf=" + value + SPACE + originOptions;
        confMap.set(key, newOptions.trim());
    }

    //TODO serverless 也许不需要，得先确认一下这里会有什么，从样例数据构建中，一下两个参数只给了newten-job.jar，这个对serverless无意义
    private void rewriteExtraClasspath(KylinConfig kylinConf, Map<String, String> sparkConf) {
        // Add extra jars to driver/executor classpath.
        // In yarn cluster mode, make sure class SparkDriverHdfsLogAppender & SparkExecutorHdfsLogAppender
        // (assembled in the kylinJobJar)
        // will be in NM container's classpath.

        // Cluster mode.
        if (isClusterMode(sparkConf)) {
            // On yarn cluster mode,
            // application jar (kylinJobJar here) would ln as '__app__.jar'.
            Set<String> sparkJars = Sets.newLinkedHashSet();
            sparkJars.add(APP_JAR_NAME);
            sparkJars.addAll(getSparkJars(kylinConf, sparkConf));
            final String jointJarNames = String.join(COLON, //
                    sparkJars.stream().map(jar -> Paths.get(jar).getFileName().toString()).collect(Collectors.toSet()));
            sparkConf.put(DRIVER_EXTRA_CLASSPATH, jointJarNames);
            sparkConf.put(EXECUTOR_EXTRA_CLASSPATH, jointJarNames);
            return;
        }

        // Client mode.
        Set<String> sparkJars = getSparkJars(kylinConf, sparkConf);
        sparkConf.put(DRIVER_EXTRA_CLASSPATH, String.join(COLON, sparkJars));
        sparkConf.put(EXECUTOR_EXTRA_CLASSPATH, String.join(COLON, //
                sparkJars.stream().map(jar -> Paths.get(jar).getFileName().toString()).collect(Collectors.toSet())));
    }

    private void rewriteDriverLog4jConf(StringBuilder sb, KylinConfig config, Map<String, String> sparkConf) {
        // https://issues.apache.org/jira/browse/SPARK-16784
        final String localLog4j = config.getLogSparkDriverPropertiesFile();
        final String remoteLog4j = Paths.get(localLog4j).getFileName().toString();
        if (isClusterMode(sparkConf) || config.getSparkMaster().startsWith("k8s")) {
            // Direct file name.
            sb.append(SPACE).append("-Dlog4j.configurationFile=").append(remoteLog4j);
        } else {
            // Use 'file:' as scheme.
            sb.append(SPACE).append("-Dlog4j.configurationFile=file:").append(localLog4j);
        }
    }

    private Set<String> getSparkJars(KylinConfig kylinConf, Map<String, String> sparkConf) {
        Set<String> jarPaths = Sets.newLinkedHashSet();
        // Client mode, application jar (kylinJobJar here) wouldn't ln as '__app__.jar'.
        // Cluster mode, application jar (kylinJobJar here) would be uploaded automatically & ln as '__app__.jar'.
        jarPaths.add(kylinConf.getKylinJobJarPath());
        jarPaths.add(kylinConf.getExtraJarsPath());
        jarPaths.add(getJars());
        jarPaths.add(getExtJar());
        jarPaths.add(sparkConf.get(SPARK_JARS_1));
        jarPaths.add(sparkConf.get(SPARK_JARS_2));

        LinkedHashSet<String> sparkJars = jarPaths.stream() //
                .filter(StringUtils::isNotEmpty) //
                .flatMap(p -> Arrays.stream(StringUtils.split(p, COMMA))) //
                .filter(jar -> jar.endsWith(".jar")) //
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return Collections.unmodifiableSet(sparkJars);
    }

    private Set<String> getSparkFiles(KylinConfig kylinConf, Map<String, String> sparkConf) {
        Set<String> filePaths = Sets.newLinkedHashSet();
        filePaths.add(kylinConf.getLogSparkAppMasterPropertiesFile());
        filePaths.add(kylinConf.getLogSparkDriverPropertiesFile());
        filePaths.add(kylinConf.getLogSparkExecutorPropertiesFile());
        filePaths.add(sparkConf.get(SPARK_FILES_1));
        filePaths.add(sparkConf.get(SPARK_FILES_2));

        LinkedHashSet<String> sparkFiles = filePaths.stream() //
                .filter(StringUtils::isNotEmpty) //
                .flatMap(p -> Arrays.stream(StringUtils.split(p, COMMA))) //
                .filter(StringUtils::isNotEmpty) //
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return Collections.unmodifiableSet(sparkFiles);
    }

}
