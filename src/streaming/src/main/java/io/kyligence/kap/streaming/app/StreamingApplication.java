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

package io.kyligence.kap.streaming.app;

import static io.kyligence.kap.common.persistence.metadata.HDFSMetadataStore.HDFS_SCHEME;
import static io.kyligence.kap.metadata.cube.model.NDataSegDetails.DATAFLOW_DETAILS_RESOURCE_ROOT;
import static org.apache.kylin.common.persistence.ResourceStore.STREAMING_RESOURCE_ROOT;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Application;
import org.apache.kylin.common.util.TimeZoneUtils;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.KylinSession;
import org.apache.spark.sql.KylinSession$;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasource.AlignmentTableStats;

import io.kyligence.kap.cluster.IClusterManager;
import io.kyligence.kap.common.persistence.metadata.HDFSMetadataStore;
import io.kyligence.kap.common.persistence.metadata.JdbcPartialAuditLogStore;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;
import io.kyligence.kap.engine.spark.job.UdfManager;
import io.kyligence.kap.engine.spark.utils.JobMetricsUtils;
import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.jobs.GracefulStopInterface;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import io.kyligence.kap.streaming.rest.RestSupport;
import io.kyligence.kap.streaming.util.JobExecutionIdHolder;
import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

@Slf4j
public abstract class StreamingApplication implements Application, GracefulStopInterface {

    protected SparkSession ss;
    protected String project;
    protected String dataflowId;
    protected String distMetaUrl;
    protected JobTypeEnum jobType;
    protected String jobId;
    protected Integer jobExecId;

    protected final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
    @Getter(lazy = true)
    private final Set<String> metaResPathSet = initMetaPathSet();

    private void prepareKylinConfig() throws Exception {
        val jobStorageUrl = StorageURL.valueOf(distMetaUrl);
        if (!jobStorageUrl.getScheme().equals(HDFSMetadataStore.HDFS_SCHEME)) {
            kylinConfig.setMetadataUrl(distMetaUrl);
            return;
        }

        //init audit log store
        val auditLogStore = new JdbcPartialAuditLogStore(kylinConfig,
                resPath -> resPath.startsWith(
                        String.format(Locale.ROOT, "/%s%s/%s", project, DATAFLOW_DETAILS_RESOURCE_ROOT, dataflowId))
                        || getMetaResPathSet().contains(resPath));

        kylinConfig.setMetadataUrl(distMetaUrl);

        Preconditions.checkState(HDFS_SCHEME.equals(kylinConfig.getMetadataUrl().getScheme()));
        val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        resourceStore.getMetadataStore().setAuditLogStore(auditLogStore);
        //begin catchup
        resourceStore.catchup();
        log.info("start job from offset:{}", auditLogStore.getLogOffset());
    }

    private Set<String> initMetaPathSet() {
        //init dump meta set
        val dumpMetaPathSet = NDataflowManager.getInstance(kylinConfig, project) //
                .getDataflow(dataflowId) //
                .collectPrecalculationResource();
        dumpMetaPathSet.add(String.format(Locale.ROOT, "/%s%s/%s", project, STREAMING_RESOURCE_ROOT, jobId));
        return dumpMetaPathSet;
    }

    protected void prepareBeforeExecute() throws ExecuteException {
        try {
            TimeZoneUtils.setDefaultTimeZone(kylinConfig);

            if (isJobOnCluster()) {
                prepareKylinConfig();
            }

            //init spark session
            getOrCreateSparkSession(KylinBuildEnv.getOrCreate(kylinConfig).sparkConf());

            //init job execution
            this.jobExecId = reportApplicationInfo();
            JobExecutionIdHolder.setJobExecutionId(jobId, jobExecId);
            startJobExecutionIdCheckThread();
        } catch (Exception e) {
            throw new ExecuteException(e);
        }

    }

    public abstract void parseParams(String[] args);

    @Override
    public void execute(String[] args) {
        try {
            parseParams(args);
            prepareBeforeExecute();
            doExecute();
        } catch (Exception e) {
            log.error("{} execute error", this.getClass().getCanonicalName(), e);
            ExceptionUtils.rethrow(e);
        }

    }

    protected abstract void doExecute() throws ExecuteException;

    public void getOrCreateSparkSession(SparkConf sparkConf) {
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
        boolean createWithSparkSession = !isJobOnCluster() && SparderEnv.isSparkAvailable();
        if (createWithSparkSession) {
            boolean isKylinSession = SparderEnv.getSparkSession() instanceof KylinSession;
            createWithSparkSession = !isKylinSession;
        }

        if (createWithSparkSession) {
            ss = sessionBuilder.getOrCreate();
        } else {
            ss = KylinSession$.MODULE$.KylinBuilder(sessionBuilder).buildCluster().getOrCreateKylinSession();
        }

        UdfManager.create(ss);
        JobMetricsUtils.registerListener(ss);
        if (isJobOnCluster()) {
            val config = KylinConfig.getInstanceFromEnv();
            Unsafe.setProperty("kylin.env", config.getDeployEnv());
        }
    }

    public void closeAuditLogStore(SparkSession ss) {
        if (isJobOnCluster()) {
            JobMetricsUtils.unRegisterListener(ss);
            val store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            try {
                store.getAuditLogStore().close();
            } catch (IOException e) {
                log.error("close audit log error", e);
            }
        }
    }

    public Integer reportApplicationInfo() {
        val buildEnv = getOrCreateKylinBuildEnv(kylinConfig);
        val appId = ss.sparkContext().applicationId();
        var trackingUrl = StringUtils.EMPTY;
        val cm = buildEnv.clusterManager();
        trackingUrl = getTrackingUrl(cm, ss);
        boolean isIpPreferred = kylinConfig.isTrackingUrlIpAddressEnabled();
        try {
            if (StringUtils.isBlank(trackingUrl)) {
                log.info("Get tracking url of application $appId, but empty url found.");
            }
            if (isIpPreferred && !StringUtils.isEmpty(trackingUrl)) {
                trackingUrl = tryReplaceHostAddress(trackingUrl);
            }
        } catch (Exception e) {
            log.error("get tracking url failed!", e);
        }
        val request = new StreamingJobUpdateRequest(project, dataflowId, jobType.name(), appId, trackingUrl);
        request.setProcessId(StreamingUtils.getProcessId());
        request.setNodeInfo(AddressUtil.getZkLocalInstance());
        try (val rest = createRestSupport(kylinConfig)) {
            val restResp = rest.execute(rest.createHttpPut("/streaming_jobs/spark"), request);
            return Integer.parseInt(restResp.getData());
        }
    }

    public KylinBuildEnv getOrCreateKylinBuildEnv(KylinConfig config) {
        return KylinBuildEnv.getOrCreate(config);
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

    public String tryReplaceHostAddress(String url) {
        String originHost = null;
        try {
            val uri = URI.create(url);
            originHost = uri.getHost();
            val hostAddress = InetAddress.getByName(originHost).getHostAddress();
            return url.replace(originHost, hostAddress);
        } catch (UnknownHostException uhe) {
            log.error("failed to get the ip address of $originHost, step back to use the origin tracking url.", uhe);
            return url;
        }
    }

    public void systemExit(int code) {
        if (isJobOnCluster()) {
            Unsafe.systemExit(code);
        }
    }

    public boolean isJobOnCluster() {
        val config = KylinConfig.getInstanceFromEnv();
        return !StreamingUtils.isLocalMode() && !config.isUTEnv();
    }

    protected void closeSparkSession() {
        if (!StreamingUtils.isLocalMode() && !ss.sparkContext().isStopped()) {
            ss.stop();
        }
    }

    public SparkSession getSparkSession() {
        return ss;
    }

    public void setSparkSession(SparkSession ss) {
        this.ss = ss;
    }

    public Map<String, String> getJobParams(StreamingJobMeta jobMeta) {
        return jobMeta.getParams();
    }

    public boolean isGracefulShutdown(String project, String uuid) {
        val config = KylinConfig.getInstanceFromEnv();
        val mgr = StreamingJobManager.getInstance(config, project);
        val meta = mgr.getStreamingJobByUuid(uuid);
        return StreamingConstants.ACTION_GRACEFUL_SHUTDOWN.equals(meta.getAction());
    }

    public boolean isRunning() {
        return !getStopFlag() && !ss.sparkContext().isStopped();
    }

    /**
     * periodic check driver's job execution id is same with meta data's job execution id
     */
    public void startJobExecutionIdCheckThread() {
        val processCheckThread = new Thread(() -> {
            val conf = KylinConfig.getInstanceFromEnv();
            val jobExecutionIdCheckInterval = conf.getStreamingJobExecutionIdCheckInterval();
            while (isRunning()) {
                try {
                    StreamingUtils.replayAuditlog();
                    val mgr = StreamingJobManager.getInstance(conf, project);
                    val meta = mgr.getStreamingJobByUuid(jobId);
                    if (!Objects.equals(jobExecId, meta.getJobExecutionId())) {
                        closeSparkSession();
                        break;
                    }
                } catch (Exception e) {
                    log.warn("check JobExecutionId error:", e);
                }
                StreamingUtils.sleep(TimeUnit.MINUTES.toMillis(jobExecutionIdCheckInterval));
            }
        });
        processCheckThread.setDaemon(true);
        processCheckThread.start();
    }

    public RestSupport createRestSupport(KylinConfig config) {
        return new RestSupport(config);
    }
}
