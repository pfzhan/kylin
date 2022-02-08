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

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.SetThreadName;
import org.springframework.stereotype.Service;

import io.kyligence.kap.tool.HDFSMetadataTool;
import io.kyligence.kap.tool.MetadataTool;
import lombok.val;

@Service
public class MetadataBackupService {

    private ExecutorService executors = Executors.newSingleThreadExecutor();

    public void backupAll() throws Exception {

        try (SetThreadName ignored = new SetThreadName("MetadataBackupWorker")) {
            String[] args = new String[] { "-backup", "-compress", "-dir", getBackupDir() };
            backup(args);
            executors.submit(this::rotateAuditLog);
        }
    }

    public void backup(String[] args) throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        HDFSMetadataTool.cleanBeforeBackup(kylinConfig);
        val metadataTool = new MetadataTool(kylinConfig);
        metadataTool.execute(args);
    }

    public void rotateAuditLog() {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        val auditLogStore = resourceStore.getAuditLogStore();
        auditLogStore.rotate();
    }

    public String backupProject(String project) throws Exception {
        val folder = LocalDateTime.now(Clock.systemDefaultZone()).format(MetadataTool.DATE_TIME_FORMATTER) + "_backup";
        String[] args = new String[] { "-backup", "-compress", "-project", project, "-folder", folder, "-dir",
                getBackupDir() };
        backup(args);
        return StringUtils.appendIfMissing(getBackupDir(), "/") + folder;
    }

    private String getBackupDir() {
        return HadoopUtil.getBackupFolder(KylinConfig.getInstanceFromEnv());

    }

}
