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

import com.google.common.collect.Maps;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.service.ConfigServiceV2;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

@Controller
@Component("configControllerV2")
@RequestMapping(value = "/config")
public class ConfigControllerV2 extends BasicController {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ConfigControllerV2.class);

    @Autowired
    @Qualifier("configServiceV2")
    private ConfigServiceV2 configServiceV2;

    @RequestMapping(value = "default", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getDefaultValue(@RequestHeader("Accept-Language") String lang, @RequestParam("key") String key) {
        KapMsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, configServiceV2.getDefaultConfigMap().get(key), "");
    }

    @RequestMapping(value = "defaults", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getDefaultConfigs(@RequestHeader("Accept-Language") String lang) {
        KapMsgPicker.setMsg(lang);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, configServiceV2.getDefaultConfigMap(), "");
    }

    /**
     *
     * @param key the name of the feature
     * @return
     * error code 001: feature_name is empty
     */
    @RequestMapping(value = "hidden_feature", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse isFeatureHidden(@RequestHeader("Accept-Language") String lang, @RequestParam("feature_name") String key) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        if (StringUtils.isEmpty(key)) {
            throw new BadRequestException(msg.getEMPTY_FEATURE_NAME());
        }

        String s = configServiceV2.getAllKylinPropertiesV2().getProperty("kap.web.hide-feature." + key);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "true".equals(s), "");
    }

    @RequestMapping(value = "spark_status", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSparkExec(@RequestHeader("Accept-Language") String lang) {
        KapMsgPicker.setMsg(lang);

        int execNum = Integer.parseInt(configServiceV2.getSparkDriverConf("spark.executor.instances"));
        Map<String, String> ret = Maps.newHashMap();

        byte[] bytes = new byte[4];
        BytesUtil.writeUnsigned(execNum, bytes, 0, bytes.length);

        byte tmp = bytes[0];
        bytes[0] = bytes[2];
        bytes[2] = tmp;
        tmp = bytes[1];
        bytes[1] = bytes[3];
        bytes[3] = tmp;
        execNum = Integer.reverse(BytesUtil.readUnsigned(bytes, 0, bytes.length));

        ret.put("v", Integer.toString(execNum));
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, ret, "");
    }
}
