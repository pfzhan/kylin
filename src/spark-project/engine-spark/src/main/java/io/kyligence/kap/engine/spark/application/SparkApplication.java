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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.engine.spark.utils.SparkConfHelper.COUNT_DISTICT;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Application;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.TimeZoneUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.application.NoRetryException;
import org.apache.spark.sql.KylinSession;
import org.apache.spark.sql.KylinSession$;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasource.AlignmentTableStats;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.apache.spark.util.Utils;
import org.apache.spark.utils.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.cluster.IClusterManager;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.job.BuildJobInfos;
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;
import io.kyligence.kap.engine.spark.job.LogJobInfoUtils;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.engine.spark.job.SparkJobConstants;
import io.kyligence.kap.engine.spark.job.UdfManager;
import io.kyligence.kap.engine.spark.utils.JobMetricsUtils;
import io.kyligence.kap.engine.spark.utils.SparkConfHelper;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import lombok.val;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public abstract class SparkApplication implements Application, IKeep {
    private static final Logger logger = LoggerFactory.getLogger(SparkApplication.class);
    private Map<String, String> params = Maps.newHashMap();
    protected volatile KylinConfig config;
    protected volatile String jobId;
    protected SparkSession ss;
    protected String project;
    protected int layoutSize = -1;
    protected BuildJobInfos infos;
    /**
     * path for spark app args on HDFS
     */
    protected String path;

    public void execute(String[] args) {
        try {
            path = args[0];
            String argsLine = readArgsFromHDFS();
            params = JsonUtil.readValueAsMap(argsLine);
            logger.info("Execute {} with args : {}", this.getClass().getName(), argsLine);
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

    /**
     * http request the spark job controller
     *
     * @param json
     */
    public Boolean updateSparkJobInfo(String url, String json) {
        String serverAddress = System.getProperty("spark.driver.rest.server.address", "127.0.0.1:7070");
        String requestApi = String.format(Locale.ROOT, "http://%s%s", serverAddress, url);

        try {
            Long timeout = config.getUpdateJobInfoTimeout();
            RequestConfig defaultRequestConfig = RequestConfig.custom().setSocketTimeout(timeout.intValue())
                    .setConnectTimeout(timeout.intValue()).setConnectionRequestTimeout(timeout.intValue())
                    .setStaleConnectionCheckEnabled(true).build();
            CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(defaultRequestConfig).build();
            HttpPut httpPut = new HttpPut(requestApi);
            httpPut.addHeader(HttpHeaders.CONTENT_TYPE, HTTP_VND_APACHE_KYLIN_JSON);
            httpPut.setEntity(new StringEntity(json, StandardCharsets.UTF_8));

            HttpResponse response = httpClient.execute(httpPut);
            int code = response.getStatusLine().getStatusCode();
            if (code == HttpStatus.SC_OK) {
                return true;
            } else {
                InputStream inputStream = response.getEntity().getContent();
                String responseContent = IOUtils.toString(inputStream);
                logger.warn("update spark job failed, info: {}", responseContent);
            }
        } catch (Exception e) {
            logger.error("http request {} failed!", requestApi, e);
        }
        return false;
    }

    public String readArgsFromHDFS() {
        val fs = HadoopUtil.getFileSystem(path);
        String argsLine = null;
        Path filePath = new Path(path);
        try (FSDataInputStream inputStream = fs.open(filePath)) {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, Charset.defaultCharset()));
            argsLine = br.readLine();
        } catch (IOException e) {
            logger.error("Error occurred when reading args file: {}", path, e);
        }
        return argsLine;
    }

    /**
     * get tracking url by application id
     *
     * @param sparkSession build sparkSession
     * @return
     */
    public String getTrackingUrl(IClusterManager cm, SparkSession sparkSession) {
        return cm.getBuildTrackingUrl(sparkSession);
    }

    /**
     * when
     * update spark job extra info, link yarn_application_tracking_url & yarn_application_id
     */
    public Boolean updateSparkJobExtraInfo(String url, String project, String jobId, Map<String, String> extraInfo) {
        Map<String, String> payload = new HashMap<>(5);
        payload.put("project", project);
        payload.put("job_id", jobId);
        payload.put("task_id", System.getProperty("spark.driver.param.taskId", jobId));
        payload.putAll(extraInfo);

        try {
            String payloadJson = JsonUtil.writeValueAsString(payload);
            int retry = 3;
            for (int i = 0; i < retry; i++) {
                if (updateSparkJobInfo(url, payloadJson)) {
                    return Boolean.TRUE;
                }
                Thread.sleep(3000);
                logger.warn("retry request rest api update spark extra job info");
            }
        } catch (Exception e) {
            logger.error("update spark job extra info failed!", e);
        }

        return Boolean.FALSE;
    }

    private String tryReplaceHostAddress(String url) {
        String originHost = null;
        try {
            URI uri = URI.create(url);
            originHost = uri.getHost();
            String hostAddress = InetAddress.getByName(originHost).getHostAddress();
            return url.replace(originHost, hostAddress);
        } catch (UnknownHostException uhe) {
            logger.error(
                    "failed to get the ip address of " + originHost + ", step back to use the origin tracking url.",
                    uhe);
            return url;
        }
    }

    private Map<String, String> getTrackingInfo(IClusterManager cm, boolean ipAddressPreferred) {
        String applicationId = ss.sparkContext().applicationId();
        Map<String, String> extraInfo = new HashMap<>();
        extraInfo.put("yarn_app_id", applicationId);
        try {
            String trackingUrl = getTrackingUrl(cm, ss);
            if (StringUtils.isBlank(trackingUrl)) {
                logger.warn("Get tracking url of application {}, but empty url found.", applicationId);
                return extraInfo;
            }
            if (ipAddressPreferred) {
                trackingUrl = tryReplaceHostAddress(trackingUrl);
            }
            extraInfo.put("yarn_app_url", trackingUrl);
        } catch (Exception e) {
            logger.error("get tracking url failed!", e);
        }
        return extraInfo;
    }

    protected final void execute() throws Exception {
        String hdfsMetalUrl = getParam(NBatchConstants.P_DIST_META_URL);
        jobId = getParam(NBatchConstants.P_JOB_ID);
        project = getParam(NBatchConstants.P_PROJECT_NAME);
        if (getParam(NBatchConstants.P_LAYOUT_IDS) != null) {
            layoutSize = StringUtils.split(getParam(NBatchConstants.P_LAYOUT_IDS), ",").length;
        }
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoCloseConfig = KylinConfig
                .setAndUnsetThreadLocalConfig(KylinConfig.loadKylinConfigFromHdfs(hdfsMetalUrl))) {
            config = autoCloseConfig.get();
            // init KylinBuildEnv
            KylinBuildEnv buildEnv = KylinBuildEnv.getOrCreate(config);
            infos = KylinBuildEnv.get().buildJobInfos();
            infos.recordJobStepId(System.getProperty("spark.driver.param.taskId", jobId));
            HadoopUtil.setCurrentConfiguration(new Configuration());
            SparkConf sparkConf = buildEnv.sparkConf();
            if (isJobOnCluster(sparkConf)) {
                if (config.getSparkConfigOverride().size() > 0) {
                    Map<String, String> sparkConfigOverride = getSparkConfigOverride(config);
                    for (Map.Entry<String, String> entry : sparkConfigOverride.entrySet()) {
                        sparkConf.set(entry.getKey(), entry.getValue());
                    }
                    logger.info("Override user-defined spark conf: {}",
                            JsonUtil.writeValueAsString(sparkConfigOverride));
                }
                if (config.isAutoSetSparkConf()) {
                    logger.info("Set spark conf automatically.");
                    try {
                        autoSetSparkConf(sparkConf);
                    } catch (Exception e) {
                        logger.warn("Auto set spark conf failed. Load spark conf from system properties", e);
                    }
                }
            }

            TimeZoneUtils.setDefaultTimeZone(config);

            // wait until resource is enough
            waiteForResource(sparkConf, buildEnv);

            logger.info("Prepare job environment");
            ss = createSpark(sparkConf);

            if (!config.isUTEnv() && (!sparkConf.get("spark.master").startsWith("k8s"))) {
                updateSparkJobExtraInfo("/kylin/api/jobs/spark", project, jobId,
                        getTrackingInfo(buildEnv.clusterManager(), config.isTrackingUrlIpAddressEnabled()));
            }

            // for spark metrics
            JobMetricsUtils.registerListener(ss);

            //#8341
            SparderEnv.setSparkSession(ss);
            UdfManager.create(ss);

            if (!config.isUTEnv()) {
                Unsafe.setProperty("kylin.env", config.getDeployEnv());
            }
            logger.info("Start job");
            infos.startJob();
            extraInit();
            doExecute();
            // Output metadata to another folder
            val resourceStore = ResourceStore.getKylinMetaStore(config);
            val outputConfig = KylinConfig.createKylinConfig(config);
            outputConfig.setMetadataUrl(getParam(NBatchConstants.P_OUTPUT_META_URL));
            MetadataStore.createMetadataStore(outputConfig).dump(resourceStore);
        } finally {
            if (infos != null) {
                infos.jobEnd();
            }
            if (ss != null && !ss.conf().get("spark.master").startsWith("local")) {
                JobMetricsUtils.unRegisterListener(ss);
                ss.stop();
            }
        }
    }

    private SparkSession createSpark(SparkConf sparkConf) {
        SparkSession.Builder sessionBuilder = SparkSession.builder()
                .withExtensions(new AbstractFunction1<SparkSessionExtensions, BoxedUnit>() {
                    @Override
                    public BoxedUnit apply(SparkSessionExtensions v1) {
                        v1.injectPostHocResolutionRule(new AbstractFunction1<SparkSession, Rule<LogicalPlan>>() {
                            @Override
                            public Rule<LogicalPlan> apply(SparkSession session) {
                                return new AlignmentTableStats(session);
                            }
                        });
                        return BoxedUnit.UNIT;
                    }
                }).enableHiveSupport().config(sparkConf)
                .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        // If this is UT and SparkSession is already created, then use SparkSession.
        // Otherwise, we always use KylinSession
        boolean createWithSparkSession = !isJobOnCluster(sparkConf) && SparderEnv.isSparkAvailable();
        if (createWithSparkSession) {
            boolean isKylinSession = SparderEnv.getSparkSession() instanceof KylinSession;
            createWithSparkSession = !isKylinSession;
        }

        if (createWithSparkSession) {
            return sessionBuilder.getOrCreate();
        } else {
            return KylinSession$.MODULE$.KylinBuilder(sessionBuilder).buildCluster().getOrCreateKylinSession();
        }
    }

    public boolean isJobOnCluster(SparkConf conf) {
        return !Utils.isLocalMaster(conf) && !config.isUTEnv();
    }

    protected void extraInit() {
        //do nothing
    }

    protected abstract void doExecute() throws Exception;

    protected void onLayoutFinished(long layoutId) {
        //do nothing
    }

    protected void onExecuteFinished() {
        //do nothing
    }

    protected String calculateRequiredCores() throws Exception {
        return SparkJobConstants.DEFAULT_REQUIRED_CORES;
    }

    private void autoSetSparkConf(SparkConf sparkConf) throws Exception {
        SparkConfHelper helper = new SparkConfHelper();
        // copy user defined spark conf
        if (sparkConf.getAll() != null) {
            Arrays.stream(sparkConf.getAll()).forEach(config -> helper.setConf(config._1, config._2));
        }
        helper.setClusterManager(KylinBuildEnv.get().clusterManager());

        chooseContentSize(helper);

        helper.setOption(SparkConfHelper.LAYOUT_SIZE, Integer.toString(layoutSize));
        Map<String, String> configOverride = config.getSparkConfigOverride();
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, configOverride.get(SparkConfHelper.DEFAULT_QUEUE));
        helper.setOption(SparkConfHelper.REQUIRED_CORES, calculateRequiredCores());
        helper.setConf(COUNT_DISTICT, hasCountDistinct().toString());
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
    }

    private void waiteForResource(SparkConf sparkConf, KylinBuildEnv buildEnv) throws Exception {
        if (isJobOnCluster(sparkConf)) {
            long sleepSeconds = (long) (Math.random() * 60L);
            logger.info("Sleep {} seconds to avoid submitting too many spark job at the same time.", sleepSeconds);
            infos.startWait();
            Thread.sleep(sleepSeconds * 1000);
            try {
                while (!ResourceUtils.checkResource(sparkConf, buildEnv.clusterManager())) {
                    long waitTime = (long) (Math.random() * 10 * 60);
                    logger.info("Current available resource in cluster is not sufficient, wait {} seconds.", waitTime);
                    Thread.sleep(waitTime * 1000L);
                }
            } catch (NoRetryException e) {
                throw e;
            } catch (Exception e) {
                logger.warn("Error occurred when check resource. Ignore it and try to submit this job.", e);
            }
            infos.endWait();
        }
    }

    protected void chooseContentSize(SparkConfHelper helper) {
        Path shareDir = config.getJobTmpShareDir(project, jobId);
        // add content size with unit
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, chooseContentSize(shareDir));
    }

    protected String chooseContentSize(Path shareDir) {
        // return size with unit
        return ResourceDetectUtils.getMaxResourceSize(shareDir) + "b";
    }

    protected Boolean hasCountDistinct() throws IOException {
        Path countDistinct = new Path(config.getJobTmpShareDir(project, jobId),
                ResourceDetectUtils.countDistinctSuffix());
        FileSystem fileSystem = countDistinct.getFileSystem(HadoopUtil.getCurrentConfiguration());
        Boolean exist;
        if (fileSystem.exists(countDistinct)) {
            exist = ResourceDetectUtils.readResourcePathsAs(countDistinct);
        } else {
            exist = false;
            logger.debug("File count_distinct.json doesn't exist, set hasCountDistinct to false.");
        }
        logger.debug("Exist count distinct measure: {}", exist);
        return exist;
    }

    public void logJobInfo() {
        try {
            logger.info(generateInfo());
            Map<String, String> extraInfo = new HashMap<>();
            extraInfo.put("yarn_job_wait_time", ((Long) KylinBuildEnv.get().buildJobInfos().waitTime()).toString());
            extraInfo.put("yarn_job_run_time", ((Long) KylinBuildEnv.get().buildJobInfos().buildTime()).toString());
            updateSparkJobExtraInfo("/kylin/api/jobs/wait_and_run_time", project, jobId, extraInfo);
        } catch (Exception e) {
            logger.warn("Error occurred when generate job info.", e);
        }
    }

    protected String generateInfo() {
        return LogJobInfoUtils.sparkApplicationInfo();
    }

    protected Set<String> getIgnoredSnapshotTables() {
        return NSparkCubingUtil.toIgnoredTableSet(getParam(NBatchConstants.P_IGNORED_SNAPSHOT_TABLES));
    }

    private Map<String, String> getSparkConfigOverride(KylinConfig config) {
        Map<String, String> sparkConfigOverride = config.getSparkConfigOverride();
        String sparkExecutorExtraJavaOptionsKey = "spark.executor.extraJavaOptions";
        StringBuilder sb = new StringBuilder();
        if (sparkConfigOverride.containsKey(sparkExecutorExtraJavaOptionsKey)) {
            sb.append(sparkConfigOverride.get(sparkExecutorExtraJavaOptionsKey));
        }
        sb.append(String.format(Locale.ROOT, " -Dkylin.dictionary.globalV2-store-class-name=%s ",
                config.getGlobalDictV2StoreImpl()));
        sparkConfigOverride.put(sparkExecutorExtraJavaOptionsKey, sb.toString());
        return sparkConfigOverride;
    }
}
