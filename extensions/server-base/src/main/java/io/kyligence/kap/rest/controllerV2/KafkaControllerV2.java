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

package io.kyligence.kap.rest.controllerV2;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.service.KafkaService;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.List;

@Controller
@RequestMapping(value = "/kafka")
public class KafkaControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(KafkaControllerV2.class);

    @Autowired
    private KafkaService kafkaService;

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTopics(@RequestHeader("Accept-Language") String lang, @RequestBody StreamingRequest streamingRequest) throws IOException {
        KapMsgPicker.setMsg(lang);

        KafkaConfig kafkaConfig = deserializeKafkaSchemalDesc(streamingRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, kafkaService.getTopics(kafkaConfig), "");
    }

    @RequestMapping(value = "{cluster}/{topic}", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getMessages(@RequestHeader("Accept-Language") String lang, @PathVariable String cluster, @PathVariable String topic, @RequestBody StreamingRequest streamingRequest) throws IOException {
        KapMsgPicker.setMsg(lang);

        KafkaConfig kafkaConfig = deserializeKafkaSchemalDesc(streamingRequest);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, kafkaService.getMessages(kafkaConfig), "");
    }

    @RequestMapping(value = "{database}.{tablename}/samples", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSamples(@RequestHeader("Accept-Language") String lang, @PathVariable String database, @PathVariable String tablename, @RequestBody List<String> messages) throws IOException {
        KapMsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, kafkaService.saveSamplesToStreamingTable(database + "." + tablename, messages), "");
    }

    @RequestMapping(value = "{database}.{tablename}/update_samples", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateSamples(@RequestHeader("Accept-Language") String lang, @PathVariable String database, @PathVariable String tablename) throws IOException {
        KapMsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, kafkaService.updateSamplesByTableName(database + "." + tablename), "");
    }

    private KafkaConfig deserializeKafkaSchemalDesc(StreamingRequest streamingRequest) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        KafkaConfig desc = null;
        try {
            logger.debug("Saving KafkaConfig " + streamingRequest.getKafkaConfig());
            desc = JsonUtil.readValue(streamingRequest.getKafkaConfig(), KafkaConfig.class);
        } catch (JsonParseException e) {
            logger.error("The KafkaConfig definition is invalid.", e);
            throw new BadRequestException(msg.getINVALID_KAFKA_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The data KafkaConfig definition is invalid.", e);
            throw new BadRequestException(msg.getINVALID_KAFKA_DEFINITION());
        }
        return desc;
    }

}
