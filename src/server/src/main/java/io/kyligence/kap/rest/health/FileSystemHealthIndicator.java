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

package io.kyligence.kap.rest.health;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.rest.config.initialize.AfterMetadataReadyEvent;

@Component
public class FileSystemHealthIndicator extends AbstractKylinHealthIndicator {
    public static final Logger logger = LoggerFactory.getLogger(FileSystemHealthIndicator.class);

    private volatile boolean isHealth = false;

    private static final ScheduledExecutorService FILE_SYSTEM_HEALTH_EXECUTOR = Executors.newScheduledThreadPool(1,
            new NamedThreadFactory("FileSystemHealthChecker"));

    @EventListener(AfterMetadataReadyEvent.class)
    public void init() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        FILE_SYSTEM_HEALTH_EXECUTOR.scheduleWithFixedDelay(this::healthCheck, 0, config.getMetadataCheckDuration(),
                TimeUnit.MILLISECONDS);
    }

    public void healthCheck() {
        try {
            checkFileSystem();
            isHealth = true;
            return;
        } catch (IOException e) {
            logger.error("File System is closed, try to clean cache.", e);
        }

        // verify again
        try {
            FileSystem.closeAll();
            checkFileSystem();
            isHealth = true;
            return;
        } catch (IOException e) {
            logger.error("File System is closed AND DID NOT RECOVER", e);
        }
        isHealth = false;
    }

    @VisibleForTesting
    public void checkFileSystem() throws IOException {
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        fileSystem.exists(new Path("/"));
    }

    @Override
    public Health health() {
        return isHealth ? Health.up().build() : Health.down().build();
    }
}
