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

package org.apache.kylin.rest.service;


import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map;

@Component("licenseInfoService")
public class LicenseInfoService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(LicenseInfoService.class);
    private KapConfig kapConfig = KapConfig.getInstanceFromEnv();
    public boolean isVolumeLimitExceeded = false;

    public Map<String, String> extractLicenseInfo() {
        Map<String, String> result = new HashMap<>();
        String lic = System.getProperty("kap.license.statement");
        result.put("kap.license.statement", lic);
        result.put("kap.version", System.getProperty("kap.version"));
        result.put("kap.dates", System.getProperty("kap.dates"));
        result.put("kap.commit", System.getProperty("kap.commit"));
        result.put("kylin.commit", System.getProperty("kylin.commit"));

        if ("true".equals(System.getProperty("kap.license.isEvaluation"))) {
            result.put("kap.license.isEvaluation", "true");
        }

        if (!StringUtils.isEmpty(System.getProperty("kap.license.serviceEnd"))) {
            result.put("kap.license.serviceEnd", System.getProperty("kap.license.serviceEnd"));
        }

        result.put("kap.license.isEvaluation", System.getProperty("kap.license.isEvaluation"));

        long total;
        try {
            total = Long.parseLong(result.get("kap.license.source.total"));
        } catch (NumberFormatException e) {
            total = Long.MAX_VALUE;
        }
        float used = Float.parseFloat(result.get("kap.license.source.used"));
        isVolumeLimitExceeded = used < total ? false : true;
        return result;
    }


}
