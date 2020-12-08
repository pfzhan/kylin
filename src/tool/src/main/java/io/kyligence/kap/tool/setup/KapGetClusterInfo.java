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

package io.kyligence.kap.tool.setup;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ShellException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kyligence.kap.cluster.SchedulerInfoCmdHelper;
import io.kyligence.kap.tool.util.HadoopConfExtractor;
import lombok.val;

public class KapGetClusterInfo {
    private static final Logger logger = LoggerFactory.getLogger(KapGetClusterInfo.class);

    private static final String YARN_METRICS_SUFFIX = "/ws/v1/cluster/metrics";
    private static final String AVAILABLE_VIRTUAL_CORE = "availableVirtualCores";
    private static final String AVAILABLE_MEMORY = "availableMB";
    private String fileName = "cluster.info";

    private String yarnMasterUrlBase;

    private Map<String, Integer> clusterMetricsMap = new HashMap<>();

    public KapGetClusterInfo() {
    }

    public KapGetClusterInfo(String fileName) {
        this.fileName = fileName;
    }

    public void extractYarnMasterHost() {
        Pattern pattern = Pattern.compile("(http://)([^:]*):([^/])*.*");
        if (yarnMasterUrlBase != null) {
            Matcher m = pattern.matcher(yarnMasterUrlBase);
            if (m.matches()) {
                return;
            }
        }
        yarnMasterUrlBase = HadoopConfExtractor.extractYarnMasterUrl(HadoopUtil.getCurrentConfiguration());
    }

    public void getYarnMetrics() throws IOException, ShellException {
        extractYarnMasterHost();
        String url = yarnMasterUrlBase + YARN_METRICS_SUFFIX;
        String command = "curl -s -k --negotiate -u : " + url;
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val patternedLogger = new BufferedLogger(logger);

        val response = config.getCliCommandExecutor().execute(command, patternedLogger).getCmd();
        if (response == null) {
            throw new IllegalStateException("Cannot get yarn metrics with url: " + yarnMasterUrlBase + YARN_METRICS_SUFFIX);
        }

        logger.info("yarn metrics response: {}", response);
        JsonNode clusterMetrics;
        try {
            clusterMetrics = new ObjectMapper().readTree(response).path("clusterMetrics");
        } catch (Exception e) {
            logger.warn("Failed to get clusterMetrics from cluster.", e);
            clusterMetrics = new ObjectMapper().readTree(SchedulerInfoCmdHelper.metricsInfo()).path("clusterMetrics");
        }
        clusterMetricsMap.put(AVAILABLE_VIRTUAL_CORE, clusterMetrics.path(AVAILABLE_VIRTUAL_CORE).intValue());
        clusterMetricsMap.put(AVAILABLE_MEMORY, clusterMetrics.path(AVAILABLE_MEMORY).intValue());
    }

    public void saveToFile() throws IOException {
        File dest = new File(fileName);
        StringBuilder buf = new StringBuilder();
        for (Map.Entry<String, Integer> element : clusterMetricsMap.entrySet()) {
            String input = element.getKey() + "=" + element.getValue();
            input += "\n";
            buf.append(input);
        }
        FileUtils.writeStringToFile(dest, buf.toString(), Charset.defaultCharset());
    }

    public static void main(String[] args) throws IOException, ShellException {
        if (args.length != 1) {
            System.out.println("Usage: KapGetClusterInfo fileName");
            System.exit(1);
        }
        KapGetClusterInfo kapSetupConcurrency = new KapGetClusterInfo(args[0]);
        kapSetupConcurrency.getYarnMetrics();
        kapSetupConcurrency.saveToFile();
        System.exit(0);
    }
}
