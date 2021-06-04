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

package io.kyligence.kap.rest.config.initialize;

import java.util.Map;
import java.util.Objects;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkContext;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.rest.service.ModelService;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;

import com.codahale.metrics.Gauge;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.metrics.MetricsTag;
import io.kyligence.kap.common.metrics.prometheus.PrometheusMetricsGroup;
import io.kyligence.kap.common.metrics.prometheus.PrometheusMetricsNameEnum;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.rest.util.ModelUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelDropAddListener {

    @Subscribe
    public void onDelete(ModelService.ModelDropEvent modelDropEvent) {
        String project = modelDropEvent.getProject();
        String modelId = modelDropEvent.getModelId();
        String modelName = modelDropEvent.getModelName();
        UnitOfWorkContext context = UnitOfWork.get();
        context.doAfterUnit(() -> {
            log.debug("delete model {} in project {}", modelId, project);
            MetricsGroup.removeModelMetrics(project, modelId);
            PrometheusMetricsGroup.removeModelMetrics(project, modelName);
        });
    }

    public static void onAdd(String project, String modelId, String modelAlias) {
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        Map<String, String> tags = Maps.newHashMap();
        tags.put(MetricsTag.MODEL.getVal(), project.concat("-").concat(modelAlias));

        MetricsGroup.newGauge(MetricsName.MODEL_SEGMENTS, MetricsCategory.PROJECT, project, tags, new GaugeWrapper() {
            @Override
            public Long getResult() {
                NDataflow df = dfManager.getDataflow(modelId);
                return df == null ? 0L : df.getSegments().size();
            }
        });
        MetricsGroup.newGauge(MetricsName.MODEL_STORAGE, MetricsCategory.PROJECT, project, tags, new GaugeWrapper() {
            @Override
            public Long getResult() {
                NDataflow df = dfManager.getDataflow(modelId);
                return df == null ? 0L : df.getStorageBytesSize();
            }
        });
        MetricsGroup.newGauge(MetricsName.MODEL_LAST_QUERY_TIME, MetricsCategory.PROJECT, project, tags,
                new GaugeWrapper() {
                    @Override
                    public Long getResult() {
                        NDataflow df = dfManager.getDataflow(modelId);
                        return df == null ? 0L : df.getLastQueryTime();
                    }
                });
        MetricsGroup.newGauge(MetricsName.MODEL_QUERY_COUNT, MetricsCategory.PROJECT, project, tags,
                new GaugeWrapper() {
                    @Override
                    public Long getResult() {
                        NDataflow df = dfManager.getDataflow(modelId);
                        return df == null ? 0L : df.getQueryHitCount();
                    }
                });
        MetricsGroup.newGauge(MetricsName.MODEL_INDEX_NUM_GAUGE, MetricsCategory.PROJECT, project, tags,
                new GaugeWrapper() {
                    @Override
                    public Long getResult() {
                        NDataflow df = dfManager.getDataflow(modelId);
                        return df == null ? 0L : df.getIndexPlan().getAllLayouts().size();
                    }
                });

        MetricsGroup.newGauge(MetricsName.MODEL_EXPANSION_RATE_GAUGE, MetricsCategory.PROJECT, project, tags, () -> {
            NDataflow df = dfManager.getDataflow(modelId);
            return df == null ? (double) 0
                    : Double.parseDouble(
                            ModelUtils.computeExpansionRate(df.getStorageBytesSize(), df.getSourceBytesSize()));
        });

        MetricsGroup.newCounter(MetricsName.MODEL_BUILD_DURATION, MetricsCategory.PROJECT, project, tags);
        MetricsGroup.newCounter(MetricsName.MODEL_WAIT_DURATION, MetricsCategory.PROJECT, project, tags);
        MetricsGroup.newHistogram(MetricsName.MODEL_BUILD_DURATION_HISTOGRAM, MetricsCategory.PROJECT, project, tags);

        // add prometheus metrics
        NExecutableManager executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);

        JobTypeEnum[] jobTypeEnums = JobTypeEnum.getTypesForPrometheus();
        PrometheusMetricsGroup.newModelGauge(PrometheusMetricsNameEnum.MODEL_JOB_EXCEED_LAST_JOB_TIME_THRESHOLD,
                project, modelAlias, executableManager, manager -> {
                    AbstractExecutable lastSuccessJob = manager.getLastSuccessExecByModel(modelId, jobTypeEnums);
                    AbstractExecutable currentRunningJob = manager.getMaxDurationRunningExecByModel(modelId, jobTypeEnums);
                    if (Objects.isNull(lastSuccessJob) || Objects.isNull(currentRunningJob)
                            || lastSuccessJob.getDuration() <= 0) {
                        return 0.0;
                    }
                    return (currentRunningJob.getDuration() - lastSuccessJob.getDuration()) / (double) lastSuccessJob.getDuration();
                });
    }

    abstract static class GaugeWrapper implements Gauge<Long> {

        public abstract Long getResult();

        @Override
        public Long getValue() {
            try {
                return getResult();
            } catch (Exception e) {
                log.error("Exception happens.", e);
            }
            return 0L;
        }
    }

}
