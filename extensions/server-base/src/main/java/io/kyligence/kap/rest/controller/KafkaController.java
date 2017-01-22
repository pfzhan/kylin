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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import io.kyligence.kap.rest.service.KafkaService;

@Controller
@RequestMapping(value = "/kafka")
public class KafkaController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);

    @Autowired
    private KafkaService kafkaService;

    @RequestMapping(value = "", method = { RequestMethod.POST })
    @ResponseBody
    public Map<String, List<String>> getTopics(@RequestBody StreamingRequest streamingRequest) {
        KafkaConfig kafkaConfig = deserializeKafkaSchemalDesc(streamingRequest);
        return kafkaService.getTopics(kafkaConfig);
    }

    @RequestMapping(value = "{cluster}/{topic}", method = { RequestMethod.POST })
    @ResponseBody
    public List<String> getMessages(@PathVariable String cluster, @PathVariable String topic, @RequestBody StreamingRequest streamingRequest) {
        KafkaConfig kafkaConfig = deserializeKafkaSchemalDesc(streamingRequest);
        return kafkaService.getMessageByTopic(cluster, topic, kafkaConfig);
    }

    @RequestMapping(value = "{database}.{tablename}/samples", method = { RequestMethod.POST })
    @ResponseBody
    public String getSamples(@PathVariable String database, @PathVariable String tablename, @RequestBody List<String> messages) throws IOException {
        return kafkaService.saveSamplesToStreamingTable(database + "." + tablename, messages);
    }

    private KafkaConfig deserializeKafkaSchemalDesc(StreamingRequest streamingRequest) {
        KafkaConfig desc = null;
        try {
            logger.debug("Saving KafkaConfig " + streamingRequest.getKafkaConfig());
            desc = JsonUtil.readValue(streamingRequest.getKafkaConfig(), KafkaConfig.class);
        } catch (JsonParseException e) {
            logger.error("The KafkaConfig definition is invalid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The data KafkaConfig definition is invalid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return desc;
    }

    private void updateRequest(StreamingRequest request, boolean success, String message) {
        request.setSuccessful(success);
        request.setMessage(message);
    }

}
