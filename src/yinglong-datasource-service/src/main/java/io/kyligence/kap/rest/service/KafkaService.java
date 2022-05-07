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

package io.kyligence.kap.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.BROKER_TIMEOUT_MESSAGE;
import static org.apache.kylin.common.exception.ServerErrorCode.STREAMING_TIMEOUT_MESSAGE;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.streaming.DataParserInfo;
import io.kyligence.kap.metadata.streaming.DataParserManager;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.source.kafka.KafkaTableUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("kafkaService")
public class KafkaService extends BasicService {

    @Autowired
    private AclEvaluate aclEvaluate;

    public void checkBrokerStatus(KafkaConfig kafkaConfig) {
        List<String> brokenBrokers = KafkaTableUtil.getBrokenBrokers(kafkaConfig);
        if (CollectionUtils.isEmpty(brokenBrokers)) {
            return;
        }

        Map<String, Object> brokenBrokersMap = Maps.newHashMap();
        brokenBrokersMap.put("failed_servers", brokenBrokers);
        throw new KylinException(BROKER_TIMEOUT_MESSAGE, MsgPicker.getMsg().getBROKER_TIMEOUT_MESSAGE())
                .withData(brokenBrokersMap);
    }

    public Map<String, List<String>> getTopics(KafkaConfig kafkaConfig, String project, final String fuzzyTopic) {
        aclEvaluate.checkProjectWritePermission(project);
        checkBrokerStatus(kafkaConfig);
        return KafkaTableUtil.getTopics(kafkaConfig, fuzzyTopic);
    }

    public List<ByteBuffer> getMessages(KafkaConfig kafkaConfig, String project, int clusterIndex) {
        aclEvaluate.checkProjectWritePermission(project);
        try {
            return KafkaTableUtil.getMessages(kafkaConfig, clusterIndex);
        } catch (TimeoutException e) {
            throw new KylinException(STREAMING_TIMEOUT_MESSAGE, MsgPicker.getMsg().getSTREAMING_TIMEOUT_MESSAGE());
        }
    }

    public Map<String, Object> getMessageTypeAndDecodedMessages(List<ByteBuffer> messages) {
        return KafkaTableUtil.getMessageTypeAndDecodedMessages(messages);
    }

    public Map<String, Object> convertSampleMessageToFlatMap(String project, KafkaConfig kafkaConfig,
            String messageType, String message) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getManager(DataParserManager.class, project).initDefault(project);
            return null;
        }, project);
        return KafkaTableUtil.convertCustomMessage(project, kafkaConfig, messageType, message);
    }

    /**
     * get parser from meta
     */
    public List<String> getParsers(String project) {
        aclEvaluate.checkProjectWritePermission(project);
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            DataParserManager dataParserManager = getManager(DataParserManager.class, project);
            dataParserManager.initDefault(project);
            return dataParserManager.listDataParserInfo().stream().map(DataParserInfo::getClassName)
                    .collect(Collectors.toList());
        }, project);
    }

    /**
     * Logical deletion, inaccessible in meta
     */
    public String removeParser(String project, String className) {
        aclEvaluate.checkProjectWritePermission(project);
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(
                () -> getManager(DataParserManager.class, project).removeParser(className).getClassName(), project);
    }

}
