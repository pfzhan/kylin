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

import java.util.Map;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.rest.controller.BasicController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Maps;

import io.kyligence.kap.rest.service.ConfigService;

@Controller
@Component("configController")
@RequestMapping(value = "/config")
public class ConfigController extends BasicController {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ConfigController.class);

    @Autowired
    private ConfigService configService;

    @RequestMapping(value = "default", method = { RequestMethod.GET })
    @ResponseBody
    public String getDefaultValue(@RequestParam("key") String key) {
        return configService.getDefaultConfigMap().get(key);
    }

    @RequestMapping(value = "defaults", method = { RequestMethod.GET })
    @ResponseBody
    public Map<String, String> getDefaultConfigs() {
        return configService.getDefaultConfigMap();
    }

    @RequestMapping(value = "spark_status", method = { RequestMethod.GET })
    @ResponseBody
    public Map<String, String> getSparkExec() {
        int execNum = Integer.parseInt(configService.getSparkDriverConf("spark.executor.instances"));
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
        return ret;
    }
}
