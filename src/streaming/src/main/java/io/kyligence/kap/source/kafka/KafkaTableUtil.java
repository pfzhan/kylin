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

package io.kyligence.kap.source.kafka;

import static io.kyligence.kap.source.kafka.CollectKafkaStats.DEFAULT_PARSER;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_BROKER_DEFINITION;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_STREAMING_MESSAGE;
import static org.apache.kylin.common.exception.ServerErrorCode.STREAMING_PARSER_ERROR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_CHECK_COLUMN_NAME_FAILED;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.streaming.DataParserInfo;
import io.kyligence.kap.metadata.streaming.DataParserManager;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.parser.AbstractDataParser;
import io.kyligence.kap.parser.loader.ParserClassLoaderState;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaTableUtil {

    private static final String COL_PATTERN = "^(?!\\d+|_)([0-9a-zA-Z_]{1,}$)";

    private KafkaTableUtil() {
    }

    // =========get message from topic==========
    public static List<ByteBuffer> getMessages(KafkaConfig kafkaConfig, int clusterIndex) {
        if (!KafkaTableUtil.validateKafkaConfig(kafkaConfig.getKafkaBootstrapServers())) {
            throw new KylinException(INVALID_BROKER_DEFINITION, MsgPicker.getMsg().getINVALID_BROKER_DEFINITION());
        }
        return CollectKafkaStats.getMessages(kafkaConfig, clusterIndex);
    }

    public static Map<String, Object> getMessageTypeAndDecodedMessages(List<ByteBuffer> messages) {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalStateException("There is no message in this topic");
        }
        List<String> samples = new ArrayList<>();
        for (ByteBuffer buffer : messages) {
            String str = StandardCharsets.UTF_8.decode(buffer).toString();
            if (StringUtils.isNotBlank(str)) {
                samples.add(str);
            }
        }

        Map<String, Object> resp = new HashMap<>();
        resp.put("message_type", CollectKafkaStats.CUSTOM_MESSAGE);
        resp.put("message", samples);
        return resp;
    }

    public static boolean validateKafkaConfig(String kafkaBootstrapServers) {
        return !StringUtils.isEmpty(kafkaBootstrapServers);
    }

    public static List<String> getBrokenBrokers(KafkaConfig kafkaConfig) {
        if (!KafkaTableUtil.validateKafkaConfig(kafkaConfig.getKafkaBootstrapServers())) {
            throw new KylinException(INVALID_BROKER_DEFINITION, MsgPicker.getMsg().getINVALID_BROKER_DEFINITION());
        }
        return CollectKafkaStats.getBrokenBrokers(kafkaConfig);
    }

    public static Map<String, List<String>> getTopics(KafkaConfig kafkaConfig, final String fuzzyTopic) {
        if (!KafkaTableUtil.validateKafkaConfig(kafkaConfig.getKafkaBootstrapServers())) {
            throw new KylinException(INVALID_BROKER_DEFINITION, MsgPicker.getMsg().getINVALID_BROKER_DEFINITION());
        }
        return CollectKafkaStats.getTopics(kafkaConfig, fuzzyTopic);
    }

    // =========convert  Message to map=========
    public static Map<String, Object> convertCustomMessage(String project, KafkaConfig kafkaConfig, String messageType,
            String message) {
        if (StringUtils.isBlank(message)) {
            throw new KylinException(INVALID_STREAMING_MESSAGE, MsgPicker.getMsg().getEMPTY_STREAMING_MESSAGE());
        }

        KafkaTableUtil.validateStreamMessageType(messageType);
        return parseCustomMessage(project, kafkaConfig, messageType, message);
    }

    private static Map<String, Object> parseCustomMessage(String project, KafkaConfig kafkaConfig, String messageType,
            String message) {
        ByteBuffer byteBuf = deserializeSampleMessage(messageType, message);
        Map<String, Object> result;
        try {
            String parserName = kafkaConfig.getParserName();
            DataParserManager dataParserManager = DataParserManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    project);
            DataParserInfo dataParserInfo = dataParserManager.getDataParserInfo(parserName);
            ParserClassLoaderState sessionState = ParserClassLoaderState.getInstance(project);
            if (!sessionState.getLoadedJars().contains(dataParserInfo.getJarPath())
                    && !StringUtils.equals(DEFAULT_PARSER, dataParserInfo.getClassName())) {
                sessionState.registerJars(Sets.newHashSet(dataParserInfo.getJarPath()));
            }
            result = AbstractDataParser.getDataParser(parserName, sessionState.getClassLoader()).process(byteBuf);
        } catch (Exception e) {
            throw new KylinException(STREAMING_PARSER_ERROR, e);
        }
        checkColName(result);
        return result;
    }

    public static void validateStreamMessageType(String messageType) {
        if (StringUtils.isBlank(messageType)) {
            throw new KylinException(INVALID_STREAMING_MESSAGE, MsgPicker.getMsg().getINVALID_STREAMING_MESSAGE_TYPE());
        }
        if (!StringUtils.equals(messageType, CollectKafkaStats.JSON_MESSAGE)
                && !StringUtils.equals(messageType, CollectKafkaStats.BINARY_MESSAGE)
                && !StringUtils.equals(messageType, CollectKafkaStats.CUSTOM_MESSAGE)) {
            throw new KylinException(INVALID_STREAMING_MESSAGE, MsgPicker.getMsg().getINVALID_STREAMING_MESSAGE_TYPE());
        }
    }

    public static ByteBuffer deserializeSampleMessage(String messageType, String message) {
        if (StringUtils.equals(messageType, CollectKafkaStats.JSON_MESSAGE)) {
            if (JsonUtil.isJson(message)) {
                return StandardCharsets.UTF_8.encode(message);
            } else {
                throw new KylinException(STREAMING_PARSER_ERROR, "Json message is not valid: " + message);
            }
        }

        if (StringUtils.equals(messageType, CollectKafkaStats.BINARY_MESSAGE)) {
            return ByteBuffer.wrap(Base64.decodeBase64(message));
        }
        if (StringUtils.equals(messageType, CollectKafkaStats.CUSTOM_MESSAGE)) {
            return StandardCharsets.UTF_8.encode(message);
        }
        throw new KylinException(STREAMING_PARSER_ERROR, "Message type is not valid: " + messageType);
    }

    public static void checkColName(Map<String, Object> inputParserMap) {
        Pattern pattern = Pattern.compile(COL_PATTERN);
        for (String colName : inputParserMap.keySet()) {
            if (!pattern.matcher(colName).matches()) {
                throw new KylinException(CUSTOM_PARSER_CHECK_COLUMN_NAME_FAILED);
            }
        }
    }
}
