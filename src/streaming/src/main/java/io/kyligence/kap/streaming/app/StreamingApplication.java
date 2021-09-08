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

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.KylinSession;
import org.apache.spark.sql.KylinSession$;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasource.AlignmentTableStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.cluster.IClusterManager;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;
import io.kyligence.kap.engine.spark.utils.JobMetricsUtils;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import io.kyligence.kap.streaming.request.StreamingJobUpdateRequest;
import io.kyligence.kap.streaming.rest.RestSupport;
import lombok.val;
import lombok.var;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

public abstract class StreamingApplication {
    private static final Logger logger = LoggerFactory.getLogger(StreamingApplication.class);

    private Map<String, Pair<String, Long>> removeSegIds = new HashMap<>();
    protected SparkSession ss;

    public void putHdfsFile(String segId, Pair<String, Long> item) {
        removeSegIds.put(segId, item);
    }

    public void clearHdfsFiles(NDataflow dataflow, AtomicLong startTime) {
        val hdfsFileScanStartTime = startTime.get();
        long now = System.currentTimeMillis();
        val intervals = KylinConfig.getInstanceFromEnv().getStreamingSegmentCleanInterval() * 60 * 60 * 1000;
        if (intervals >= 0 && now - hdfsFileScanStartTime > intervals) {
            val iter = removeSegIds.keySet().iterator();
            while (iter.hasNext()) {
                String segId = iter.next();
                if (dataflow.getSegment(segId) == null) {
                    if ((now - removeSegIds.get(segId).getValue()) > intervals) {
                        try {
                            val path = removeSegIds.get(segId).getKey();
                            logger.info("clear invalid segment: {}", path);
                            HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(path));
                            iter.remove();
                        } catch (IOException e) {
                            logger.warn(e.getMessage());
                        }
                    } else if ((now - removeSegIds.get(segId).getValue()) > intervals * 10) {
                        iter.remove();
                    }
                }
            }
            startTime.set(now);
        }
    }

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
                logger.error(e.getMessage(), e);
            }
        }
    }

    public void reportApplicationInfo(KylinConfig config, String project, String modelId, String jobType, String pid) {
        val buildEnv = getOrCreateKylinBuildEnv(config);
        reportApplicationInfo(buildEnv, project, modelId, jobType, pid);
    }

    public void reportApplicationInfo(KylinBuildEnv buildEnv, String project, String modelId, String jobType,
            String pid) {
        val appId = ss.sparkContext().applicationId();
        val config = buildEnv.kylinConfig();
        var trackingUrl = StringUtils.EMPTY;
        if (isJobOnCluster()) {
            val cm = buildEnv.clusterManager();
            trackingUrl = getTrackingUrl(cm, ss);
            boolean isIpPreferred = config.isTrackingUrlIpAddressEnabled();
            try {
                if (StringUtils.isBlank(trackingUrl)) {
                    logger.info("Get tracking url of application $appId, but empty url found.");
                }
                if (!config.isUTEnv() && isIpPreferred && !StringUtils.isEmpty(trackingUrl)) {
                    trackingUrl = tryReplaceHostAddress(trackingUrl);
                }
            } catch (Exception e) {
                logger.error("get tracking url failed!", e);
            }
            val request = new StreamingJobUpdateRequest(project, modelId, jobType, appId, trackingUrl);
            request.setProcessId(pid);
            request.setNodeInfo(AddressUtil.getZkLocalInstance());
            val rest = createRestSupport(config);
            try {
                rest.execute(rest.createHttpPut("/streaming_jobs/spark"), request);
            } finally {
                rest.close();
            }
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
            logger.error("failed to get the ip address of $originHost, step back to use the origin tracking url.", uhe);
            return url;
        }
    }

    public void systemExit(int code) {
        val config = KylinConfig.getInstanceFromEnv();
        if (!config.isUTEnv() && !StreamingUtils.isLocalMode()) {
            Unsafe.systemExit(code);
        }
    }

    public boolean isJobOnCluster() {
        val config = KylinConfig.getInstanceFromEnv();
        return !StreamingUtils.isLocalMode() && !config.isUTEnv();
    }

    protected void closeSparkSession() {
        if (isJobOnCluster() && !ss.sparkContext().isStopped()) {
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

    public RestSupport createRestSupport(KylinConfig config) {
        return new RestSupport(config);
    }
}
