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
package io.kyligence.kap.tool;

import io.kyligence.kap.common.metrics.service.InfluxDBInstance;
import io.kyligence.kap.tool.util.ToolUtil;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class InfluxDBTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final String INFLUXD_PATH = "/usr/bin/influxd";

    private InfluxDBTool() {
    }

    public static void dumpInfluxDBMetrics(File exportDir) {
        KapConfig kapConfig = KapConfig.wrap(KylinConfig.getInstanceFromEnv());
        String database = kapConfig.getMetricsDbNameWithMetadataUrlPrefix();
        dumpInfluxDB(exportDir, "system_metrics", database);
    }

    public static void dumpInfluxDBMonitorMetrics(File exportDir) {
        String database = InfluxDBInstance.generateDatabase(KylinConfig.getInstanceFromEnv());
        dumpInfluxDB(exportDir, "monitor_metrics", database);
    }

    private static void dumpInfluxDB(File exportDir, String subDir, String database) {
        File destDir = new File(exportDir, subDir);

        try {
            KapConfig kapConfig = KapConfig.wrap(KylinConfig.getInstanceFromEnv());
            String host = kapConfig.getMetricsRpcServiceBindAddress();

            File influxd = new File(INFLUXD_PATH);
            if (!influxd.exists()) {
                String influxDBHome = ToolUtil.getKylinHome() + File.separator + "influxdb";
                influxd = new File(influxDBHome + INFLUXD_PATH);
            }

            String cmd = String.format("%s backup -portable -database %s -host %s %s", influxd.getAbsolutePath(),
                    database, host, destDir.getAbsolutePath());

            FileUtils.forceMkdir(destDir);

            Pair<Integer, String> result = new CliCommandExecutor().execute(cmd, null);
            if (null != result.getSecond()) {
                logger.debug("dump InfluxDB, database: {}, info: {}", database, result.getSecond());
            }
        } catch (Exception e) {
            logger.debug("Failed to dump influxdb by database: {} ", database, e);
        }
    }
}
