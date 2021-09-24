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

import com.google.common.collect.Maps;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;

import java.util.Map;

import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_CONFIG_PREFIX;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_KAFKA_CONFIG_PREFIX;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_TABLE_REFRESH_INTERVAL;

@Slf4j
public class StreamingJobUtils {

    /**
     * kylin.properties config -> model config -> job config
     *
     * @return
     */
    public static KylinConfig getStreamingKylinConfig(final KylinConfig originalConfig, Map<String, String> jobParams, String modelId, String project) {
        KylinConfigExt kylinConfigExt;
        val dataflowId = modelId;
        if (StringUtils.isNotBlank(dataflowId)) {
            val dataflowManager = NDataflowManager.getInstance(originalConfig, project);
            kylinConfigExt = dataflowManager.getDataflow(dataflowId).getConfig();
        } else {
            val projectInstance = NProjectManager.getInstance(originalConfig).getProject(project);
            kylinConfigExt = projectInstance.getConfig();
        }

        Map<String, String> streamingJobOverrides = Maps.newHashMap();

        if (MapUtils.isNotEmpty(kylinConfigExt.getExtendedOverrides())) {
            streamingJobOverrides.putAll(kylinConfigExt.getExtendedOverrides());
        }

        //load kylin.streaming.spark-conf.*
        jobParams.entrySet().stream().filter(entry ->
                entry.getKey().startsWith(STREAMING_CONFIG_PREFIX)
        ).forEach(entry -> {
            streamingJobOverrides.put(entry.getKey(), entry.getValue());
        });

        //load kylin.streaming.kafka-conf.*
        jobParams.entrySet().stream().filter(entry ->
                entry.getKey().startsWith(STREAMING_KAFKA_CONFIG_PREFIX)
        ).forEach(entry -> {
            streamingJobOverrides.put(entry.getKey(), entry.getValue());
        });

        //load dimension table refresh conf
        jobParams.entrySet().stream().filter(entry ->
                entry.getKey().equals(STREAMING_TABLE_REFRESH_INTERVAL)
        ).forEach(entry -> streamingJobOverrides.put(entry.getKey(), entry.getValue()));

        //load spark.*
        jobParams.entrySet().stream().filter(entry ->
                !entry.getKey().startsWith(STREAMING_CONFIG_PREFIX)
        ).forEach(entry -> {
            streamingJobOverrides.put(STREAMING_CONFIG_PREFIX + entry.getKey(), entry.getValue());
        });

        return KylinConfigExt.createInstance(kylinConfigExt, streamingJobOverrides);
    }
}
