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

package io.kyligence.kap.clickhouse.job;


import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.util.HadoopUtil;
import org.mockito.Mockito;

public class HadoopMockUtil {

    public static void mockGetConfiguration(Map<String, String> overrideHadoopConfig) {
        Mockito.mockStatic(HadoopUtil.class);
        Mockito.when(HadoopUtil.getCurrentConfiguration()).thenReturn(getCurrentConfiguration(overrideHadoopConfig));
    }

    public static Configuration getCurrentConfiguration(Map<String, String> overrideHadoopConfig) {
        Configuration conf = healSickConfig(new Configuration());
        // do not cache this conf, or will affect following mr jobs
        if (overrideHadoopConfig != null) {
            overrideHadoopConfig.forEach(conf::set);
        }
        return conf;
    }

    public static Configuration healSickConfig(Configuration conf) {
        // https://issues.apache.org/jira/browse/KYLIN-953
        if (StringUtils.isBlank(conf.get("hadoop.tmp.dir"))) {
            conf.set("hadoop.tmp.dir", "/tmp");
        }
        //  https://issues.apache.org/jira/browse/KYLIN-3064
        conf.set("yarn.timeline-service.enabled", "false");

        return conf;
    }
}
