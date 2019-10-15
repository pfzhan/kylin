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

import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.metrics.NMetricsTag;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ModelDropAddListener {

    public static void onDelete(String project, String modelId) {
        log.debug("delete model {} in project {}", modelId, project);
        NMetricsGroup.removeModelMetrics(project, modelId);
    }

    public static void onAdd(String project, String modelId, String modelAlias) {
        NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataflow df = dfManager.getDataflow(modelId);
        Map<String, String> tags = Maps.newHashMap();
        tags.put(NMetricsTag.MODEL.getVal(), project.concat("-").concat(modelAlias));

        NMetricsGroup.newGauge(NMetricsName.MODEL_SEGMENTS, NMetricsCategory.PROJECT, project, tags,
                () -> df.getSegments().size());
        NMetricsGroup.newGauge(NMetricsName.MODEL_STORAGE, NMetricsCategory.PROJECT, project, tags,
                () -> dfManager.getDataflowStorageSize(modelId));
        NMetricsGroup.newGauge(NMetricsName.MODEL_LAST_QUERY_TIME, NMetricsCategory.PROJECT, project, tags,
                df::getLastQueryTime);
        NMetricsGroup.newGauge(NMetricsName.MODEL_QUERY_COUNT, NMetricsCategory.PROJECT, project, tags,
                df::getQueryHitCount);

        NMetricsGroup.newCounter(NMetricsName.MODEL_BUILD_DURATION, NMetricsCategory.PROJECT, project, tags);
        NMetricsGroup.newCounter(NMetricsName.MODEL_WAIT_DURATION, NMetricsCategory.PROJECT, project, tags);
    }


}
