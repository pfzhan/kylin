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

import java.io.File;
import java.util.Locale;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.metrics.service.MonitorDao;
import io.kyligence.kap.tool.util.ToolUtil;
import lombok.val;

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
        String database = MonitorDao.generateDatabase(KylinConfig.getInstanceFromEnv());
        dumpInfluxDB(exportDir, "monitor_metrics", database);
    }

    private static void dumpInfluxDB(File exportDir, String subDir, String database) {
        File destDir = new File(exportDir, subDir);

        try {
            KapConfig kapConfig = KapConfig.wrap(KylinConfig.getInstanceFromEnv());
            String host = kapConfig.getMetricsRpcServiceBindAddress();
            FileUtils.forceMkdir(destDir);
            logger.info("Try connect {}.", host);
            String ip = host.split(":")[0];
            int port = Integer.parseInt(host.split(":")[1]);
            if (!ToolUtil.isPortAvailable(ip, port)) {
                logger.info("Failed to Connect influxDB in {}, skip dump.", host);
                return;
            }
            logger.info("Connect successful.");

            File influxd = new File(INFLUXD_PATH);
            if (!influxd.exists()) {
                String influxDBHome = ToolUtil.getKylinHome() + File.separator + "influxdb";
                influxd = new File(influxDBHome + INFLUXD_PATH);
            }

            String cmd = String.format(Locale.ROOT, "%s backup -portable -database %s -host %s %s",
                    influxd.getAbsolutePath(), database, host, destDir.getAbsolutePath());
            logger.info("InfluxDB backup cmd is {}.", cmd);

            val result = new CliCommandExecutor().execute(cmd, null);
            if (null != result.getCmd()) {
                logger.debug("dump InfluxDB, database: {}, info: {}", database, result.getCmd());
            }
        } catch (Exception e) {
            logger.debug("Failed to dump influxdb by database: {} ", database, e);
        }
    }
}
