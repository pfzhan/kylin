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

import java.util.concurrent.Executors;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.spark.scheduler.SparkUIZombieJobCleaner;
import org.apache.spark.sql.SparderEnv;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;

import io.kyligence.kap.query.util.LoadCounter;
import io.kyligence.kap.rest.config.initialize.SparderStartEvent;
import io.kyligence.kap.rest.monitor.SparkContextCanary;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@AutoConfigureOrder
public class SparderConfiguration {

    @Async
    @EventListener(SparderStartEvent.AsyncEvent.class)
    public void initAsync(SparderStartEvent.AsyncEvent event) {
        init();
    }

    @EventListener(SparderStartEvent.SyncEvent.class)
    public void initSync(SparderStartEvent.SyncEvent event) {
        init();
    }

    public void init() {
        SparderEnv.init();
        if (KylinConfig.getInstanceFromEnv().isCleanSparkUIZombieJob()) {
            SparkUIZombieJobCleaner.regularClean();
        }
        if (System.getProperty("spark.local", "false").equals("true")) {
            log.debug("spark.local=true");
            return;
        }

        // monitor Spark
        if (KapConfig.getInstanceFromEnv().getSparkCanaryEnable()) {
            val service = Executors.newSingleThreadScheduledExecutor();
            SparkContextCanary.getInstance().init(service);
            LoadCounter.getInstance().init(service);
        }
    }

}
