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

package io.kyligence.kap.tool.util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HadoopConfExtractor {
    private static final Logger logger = LoggerFactory.getLogger(HadoopConfExtractor.class);

    private HadoopConfExtractor() {}

    public static String extractYarnMasterUrl(Configuration conf) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String yarnStatusCheckUrl = config.getYarnStatusCheckUrl();
        Pattern pattern = Pattern.compile("(http(s)?://)([^:]*):([^/])*.*");
        if (yarnStatusCheckUrl != null) {
            Matcher m = pattern.matcher(yarnStatusCheckUrl);
            if (m.matches()) {
                return m.group(1) + m.group(2) + ":" + m.group(3);
            }
        }

        logger.info("kylin.job.yarn-app-rest-check-status-url is not set, read from hadoop configuration");

        String webappConfKey;
        String defaultAddr;
        if (YarnConfiguration.useHttps(conf)) {
            webappConfKey = YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS;
            defaultAddr = YarnConfiguration.DEFAULT_RM_WEBAPP_HTTPS_ADDRESS;
        } else {
            webappConfKey = YarnConfiguration.RM_WEBAPP_ADDRESS;
            defaultAddr = YarnConfiguration.DEFAULT_RM_WEBAPP_ADDRESS;
        }

        String rmWebHost;
        if (HAUtil.isHAEnabled(conf)) {
            YarnConfiguration yarnConf = new YarnConfiguration(conf);
            String active = RMHAUtils.findActiveRMHAId(yarnConf);
            rmWebHost = HAUtil.getConfValueForRMInstance(HAUtil.addSuffix(webappConfKey, active), defaultAddr,
                    yarnConf);
        } else {
            rmWebHost = HAUtil.getConfValueForRMInstance(webappConfKey, defaultAddr, conf);
        }

        if (StringUtils.isEmpty(rmWebHost)) {
            return null;
        }
        if (!rmWebHost.startsWith("http://") && !rmWebHost.startsWith("https://")) {
            rmWebHost = (YarnConfiguration.useHttps(conf) ? "https://" : "http://") + rmWebHost;
        }
        Matcher m = pattern.matcher(rmWebHost);
        Preconditions.checkArgument(m.matches(), "Yarn master URL not found.");
        logger.info("yarn master url: {} ", rmWebHost);
        return rmWebHost;
    }
}
