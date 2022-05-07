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

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.streaming.DataParserInfo;
import io.kyligence.kap.metadata.streaming.DataParserManager;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.utils.StreamingUtils;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import io.kyligence.kap.streaming.metadata.StreamingJobMeta;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSparkJobLauncher implements SparkJobLauncher {

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

    protected DataParserManager dataParserManager;
    protected DataParserInfo dataParserInfo;

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
        this.dataParserManager = DataParserManager.getInstance(config, project);
        NDataModel model = NDataModelManager.getInstance(config, project).getDataModelDesc(this.modelId);
        this.dataParserInfo = this.dataParserManager
                .getDataParserInfo(model.getRootFactTable().getTableDesc().getKafkaConfig().getParserName());
        if (StringUtils.isEmpty(kylinJobJar) && !config.isUTEnv()) {
            throw new IllegalArgumentException("Missing kylin job jar");
        }
        this.launcher = new SparkLauncher(env);
        log.info("The {} - {} initialized successfully...", jobType, jobId);
    }

    public abstract void launch();

    public abstract void stop();

    protected static String javaPropertyFormatter(@Nonnull String key, @Nullable String value) {
        Preconditions.checkNotNull(key, "the key of java property cannot be empty");
        return String.format(Locale.ROOT, " -D%s=%s ", key, value);
    }

    protected static Map<String, String> getStreamingSparkConfig(KylinConfig config) {
        return config.getStreamingSparkConfigOverride().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("spark."))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    protected static Map<String, String> getStreamingKafkaConfig(KylinConfig config) {
        return config.getStreamingKafkaConfigOverride();
    }

}
