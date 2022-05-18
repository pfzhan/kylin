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

import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_CONFIG_PREFIX;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_KAFKA_CONFIG_PREFIX;
import static io.kyligence.kap.streaming.constants.StreamingConstants.STREAMING_TABLE_REFRESH_INTERVAL;
import static org.apache.kylin.common.exception.ServerErrorCode.READ_KAFKA_JAAS_FILE_ERROR;

import java.io.File;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJobUtils {
    private static final Logger logger = Logger.getLogger(StreamingJobUtils.class);

    /**
     * kylin.properties config -> model config -> job config
     *
     * @return
     */
    public static KylinConfig getStreamingKylinConfig(final KylinConfig originalConfig, Map<String, String> jobParams,
            String modelId, String project) {
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
        jobParams.entrySet().stream().filter(entry -> entry.getKey().startsWith(STREAMING_CONFIG_PREFIX))
                .forEach(entry -> streamingJobOverrides.put(entry.getKey(), entry.getValue()));

        //load kylin.streaming.kafka-conf.*
        jobParams.entrySet().stream().filter(entry -> entry.getKey().startsWith(STREAMING_KAFKA_CONFIG_PREFIX))
                .forEach(entry -> streamingJobOverrides.put(entry.getKey(), entry.getValue()));

        //load dimension table refresh conf
        jobParams.entrySet().stream().filter(entry -> entry.getKey().equals(STREAMING_TABLE_REFRESH_INTERVAL))
                .forEach(entry -> streamingJobOverrides.put(entry.getKey(), entry.getValue()));

        //load spark.*
        jobParams.entrySet().stream().filter(entry -> !entry.getKey().startsWith(STREAMING_CONFIG_PREFIX)).forEach(
                entry -> streamingJobOverrides.put(STREAMING_CONFIG_PREFIX + entry.getKey(), entry.getValue()));

        return KylinConfigExt.createInstance(kylinConfigExt, streamingJobOverrides);
    }

    public static String extractKafkaSaslJaasConf() {
        val kapConfig = KapConfig.getInstanceFromEnv();
        if (!kapConfig.isKafkaJaasEnabled()) {
            return null;
        }
        File file = new File(kapConfig.getKafkaJaasConfPath());
        try {
            val text = FileUtils.readFileToString(file);
            int kafkaClientIdx = text.indexOf("KafkaClient");
            if (StringUtils.isNotEmpty(text) && kafkaClientIdx != -1) {
                return text.substring(text.indexOf("{") + 1, text.indexOf("}")).trim();
            }
        } catch (Exception e) {
            logger.error("read kafka jaas file error ", e);
        }
        throw new KylinException(READ_KAFKA_JAAS_FILE_ERROR, MsgPicker.getMsg().getReadKafkaJaasFileError());
    }

}
