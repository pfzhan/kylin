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
package io.kyligence.kap.rest;

import java.net.URI;
import java.util.Map;

import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.history.HistoryServer;
import org.apache.spark.deploy.history.HistoryServerBuilder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "kylin.history-server.enable", havingValue = "true")
public class SparkHistoryServer {

    @Bean("historyServer")
    @SneakyThrows
    public HistoryServer createHistoryServer() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        SparkConf sparkConf = new SparkConf();
        Map<String, String> sparkConfigOverride = config.getSparkConfigOverride();
        for (Map.Entry<String, String> entry : sparkConfigOverride.entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
        }

        String logDir = sparkConf.get("spark.eventLog.dir");

        Path logPath = new Path(new URI(logDir).getPath());
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (!fs.exists(logPath)) {
            fs.mkdirs(logPath);
        }
        sparkConf.set("spark.history.fs.logDirectory", sparkConf.get("spark.eventLog.dir"));
        return HistoryServerBuilder.createHistoryServer(sparkConf);
    }
}
