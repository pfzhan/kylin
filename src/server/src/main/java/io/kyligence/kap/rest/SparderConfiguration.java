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

import java.io.File;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.spark.scheduler.SparkInfoCollector;
import org.apache.spark.sql.SparderEnv;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import io.kyligence.kap.rest.config.initialize.AppInitializedEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@AutoConfigureOrder
public class SparderConfiguration {

    @EventListener(AppInitializedEvent.class)
    public void init(AppInitializedEvent event) {
        SparderEnv.init();
        SparkInfoCollector.collectSparkInfo();
        if (System.getProperty("spark.local", "false").equals("true")) {
            log.debug("spark.local=true");
            return;
        }

        // monitor Spark
        SparkContextCanary.init();

        // write appid to ${KYLIN_HOME} or ${user.dir}
        final String kylinHome = StringUtils.defaultIfBlank(KylinConfig.getKylinHome(), "./");
        final File appidFile = Paths.get(kylinHome, "appid").toFile();
        String appid = null;
        try {
            appid = SparderEnv.getSparkSession().sparkContext().applicationId();
            FileUtils.writeStringToFile(appidFile, appid);
            log.info("spark context appid is {}", appid);
        } catch (Exception e) {
            log.error("Failed to generate spark context appid[{}] file", StringUtils.defaultString(appid), e);
        }
    }

}
