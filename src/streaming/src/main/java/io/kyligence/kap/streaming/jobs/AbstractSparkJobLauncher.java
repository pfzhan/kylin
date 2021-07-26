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

package io.kyligence.kap.streaming.jobs;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;

public abstract class AbstractSparkJobLauncher implements SparkJobLauncher {
    private static final Logger logger = LoggerFactory.getLogger(AbstractSparkJobLauncher.class);

    protected KylinConfig config;

    protected String project;
    protected String modelId;
    protected String jobId;

    protected SparkAppHandle handler;
    protected SparkAppHandle.Listener listener;
    protected Map<String, String> env;

    protected StreamingJobManager streamingJobManager;
    protected StreamingJobMeta strmJob;
    protected JobTypeEnum jobType;

    protected String kylinJobJar;
    protected SparkLauncher launcher;

    public void init(String project, String modelId, JobTypeEnum jobType) {
        this.project = project;
        this.modelId = modelId;
        this.jobId = StreamingUtils.getJobId(modelId, jobType.name());
        this.jobType = jobType;
        this.env = Maps.newHashMap();
        this.config = KylinConfig.getInstanceFromEnv();
        this.kylinJobJar = config.getStreamingJobJarPath();
        this.listener = new StreamingJobListener(project, jobId);
        this.streamingJobManager = StreamingJobManager.getInstance(config, project);
        this.strmJob = streamingJobManager.getStreamingJobByUuid(StreamingUtils.getJobId(modelId, jobType.name()));
        env.put(StreamingConstants.HADOOP_CONF_DIR, HadoopUtil.getHadoopConfDir());
        if (StringUtils.isEmpty(kylinJobJar) && !config.isUTEnv()) {
            throw new IllegalArgumentException("Missing kylin job jar");
        }
        this.launcher = new SparkLauncher(env);
        logger.info("The streaming job {} initialized successfully...", jobId);
    }

    public abstract void launch();

    public abstract void stop();

    protected static String javaPropertyFormatter(@Nonnull String key, @Nullable String value) {
        Preconditions.checkNotNull(key, "the key of java property cannot be empty");
        return String.format(Locale.ROOT, " -D%s=%s ", key, value);
    }

    protected static Map<String, String> getStreamingSparkConfig(KylinConfig config) {
        return config.getStreamingSparkConfigOverride().entrySet().stream().filter(entry -> entry.getKey().startsWith("spark."))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
