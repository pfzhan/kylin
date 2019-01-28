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
import java.util.Comparator;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.kyligence.kap.tool.MetadataTool;
import lombok.val;

@Service
public class MetadataBackupService {

    @Scheduled(cron = "${kylin.metadata.backup-cron:0 0 0 * * *}")
    public void backupAll() throws Exception {

        String oldTheadName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName("MetadataBackupWorker");

            String[] args = new String[] { "-backup", "-dir", getBackupDir() };
            backup(args);
        } finally {
            Thread.currentThread().setName(oldTheadName);
        }
    }

    public void backup(String[] args) throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        cleanBeforeBackup(kylinConfig);
        val metadataTool = new MetadataTool(kylinConfig);
        metadataTool.execute(args);
    }

    public String backupProject(String project) throws Exception {
        val folder = LocalDateTime.now().format(MetadataTool.DATE_TIME_FORMATTER) + "_backup";
        String[] args = new String[] { "-backup", "-project", project, "-folder", folder, "-dir", getBackupDir() };
        backup(args);
        return StringUtils.appendIfMissing(getBackupDir(), "/") + folder;
    }

    private String getBackupDir() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        return StringUtils.appendIfMissing(kylinConfig.getHdfsWorkingDirectory(), "/") + "_backup";

    }

    public void cleanBeforeBackup(KylinConfig kylinConfig) throws IOException {
        val rootMetadataBackupPath = new Path(getBackupDir());
        val fs = HadoopUtil.getFileSystem(rootMetadataBackupPath);
        if (!fs.exists(rootMetadataBackupPath)) {
            fs.mkdirs(rootMetadataBackupPath);
            fs.close();
            return;
        }

        int childrenSize = fs.listStatus(rootMetadataBackupPath).length;

        while (childrenSize >= kylinConfig.getMetadataBackupCountThreshold()) {
            // remove the oldest backup metadata dir
            val maybeOldest = Stream.of(fs.listStatus(rootMetadataBackupPath))
                    .min(Comparator.comparing(FileStatus::getModificationTime));
            if (maybeOldest.isPresent()) {
                fs.delete(maybeOldest.get().getPath(), true);
            }

            childrenSize--;
        }

        fs.close();
    }

}
