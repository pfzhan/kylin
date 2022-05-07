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

package io.kyligence.kap.rest.controller;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.metadata.streaming.KafkaConfig;
import io.kyligence.kap.rest.request.StreamingRequest;
import io.kyligence.kap.rest.service.KafkaService;

@Controller
@RequestMapping(value = "/api/kafka", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class KafkaController extends NBasicController {

    @Autowired
    @Qualifier("kafkaService")
    private KafkaService kafkaService;

    @PostMapping(value = "topics", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Map<String, List<String>>> getTopics(@RequestBody StreamingRequest streamingRequest) {
        checkStreamingEnabled();
        KafkaConfig kafkaConfig = streamingRequest.getKafkaConfig();
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                kafkaService.getTopics(kafkaConfig, streamingRequest.getProject(), streamingRequest.getFuzzyKey()), "");
    }

    @PostMapping(value = "messages", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> getMessages(@RequestBody StreamingRequest streamingRequest) {
        checkStreamingEnabled();
        KafkaConfig kafkaConfig = streamingRequest.getKafkaConfig();
        List<ByteBuffer> messages = kafkaService.getMessages(kafkaConfig, streamingRequest.getProject(),
                streamingRequest.getClusterIndex());
        if (messages == null || messages.isEmpty()) {
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, Maps.newHashMap(),
                    "There is no message in this topic");
        }
        Map<String, Object> resp = kafkaService.getMessageTypeAndDecodedMessages(messages);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, resp, "");
    }

    @PostMapping(value = "convert", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<Map<String, Object>> convertMessageToFlatMap(
            @RequestBody StreamingRequest streamingRequest) {
        checkStreamingEnabled();
        KafkaConfig kafkaConfig = streamingRequest.getKafkaConfig();
        String message = streamingRequest.getMessage();
        String messageType = streamingRequest.getMessageType();
        Map<String, Object> result = kafkaService.convertSampleMessageToFlatMap(streamingRequest.getProject(),
                kafkaConfig, messageType, message);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, result, "");
    }

    @GetMapping(value = "parsers", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<List<String>> getParser(@RequestParam("project") String project) {
        checkStreamingEnabled();
        String projectName = checkProjectName(project);
        List<String> classList = kafkaService.getParsers(projectName);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, classList, "");
    }

    @DeleteMapping(value = "parser", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
    @ResponseBody
    public EnvelopeResponse<String> removeParser(@RequestParam("project") String project,
            @RequestParam("class_name") String className) {
        checkStreamingEnabled();
        String projectName = checkProjectName(project);
        String removedClassName = kafkaService.removeParser(projectName, className);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, removedClassName, "");
    }

}