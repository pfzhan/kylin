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
package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.image.HDFSImageStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.kyligence.kap.common.persistence.transaction.mq.EventStore;
import io.kyligence.kap.metadata.project.UnitOfAllWorks;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ImageArchiveService {

    @Scheduled(cron = "${kylin.image.archive-cron:0 0 0 * * *}") // default at 00:00 everyday
    public void archive() {
        val config = KylinConfig.getInstanceFromEnv();
        val eventStore = EventStore.getInstance(config);
        if (!config.getMetadataUrl().getScheme().equals(HDFSImageStore.HDFS_SCHEME)) {
            log.info("scheme {} is not HDFS", config.getMetadataUrl().getScheme());
            return;
        }
        UnitOfAllWorks.doInTransaction(() -> {
            eventStore.withConsumerLock(() -> {
                backupAndClean(config);
                dump(config);
            });
            return 0;
        });
    }

    private void backupAndClean(KylinConfig config) throws IOException {
        val currentImagePath = new Path(HadoopUtil.getLatestImagePath(config));
        val rootImagePath = currentImagePath.getParent();
        val fs = HadoopUtil.getFileSystem(rootImagePath);

        val childrenSize = fs.listStatus(rootImagePath).length;
        if (childrenSize >= config.getImageCountThreshold()) {
            // remove the oldest image
            val maybeOldest = Stream.of(fs.listStatus(rootImagePath, path -> !path.getName().equals("latest")))
                    .min(Comparator.comparing(FileStatus::getModificationTime));
            if (maybeOldest.isPresent()) {
                fs.delete(maybeOldest.get().getPath(), true);
            }
        }

        fs.rename(currentImagePath, new Path(rootImagePath,
                "archive-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss"))));
    }

    private void dump(KylinConfig config) throws Exception {
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        val snapshotStore = ResourceStore.createImageStore(config);
        snapshotStore.dump(resourceStore);
        val eventStore = EventStore.getInstance(config);
        snapshotStore.dump(eventStore);
    }
}
