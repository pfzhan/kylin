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

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

public class MetadataBackupServiceTest extends NLocalFileMetadataTestCase {

    private MetadataBackupService metadataBackupService = new MetadataBackupService();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void init() {
        staticCreateTestMetadata();
    }

    @After
    public void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testBackup() throws Exception {
        val junitFolder = temporaryFolder.getRoot();

        val kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.env.hdfs-working-dir", junitFolder.getAbsolutePath());
        kylinConfig.setMetadataUrl("metadata_backup_ut_test");

        //1.assert there is no metadata dir in root dir before backup,the root dir is junitFolder.getAbsolutePath()
        val rootPath = new Path(kylinConfig.getHdfsWorkingDirectory()).getParent();
        val rootFS = HadoopUtil.getFileSystem(rootPath);
        Assertions.assertThat(rootFS.listStatus(rootPath)).hasSize(0);
        rootFS.close();

        //2.execute backup()
        metadataBackupService.backupAll();

        //3.assert there is a metadata dir in root metadata dir after backup,the metadata dir location is junitFolder.getAbsolutePath()/metadata_backup_ut_test/backup/LocalDateTime/metadata
        val rootMetadataPath = new Path(kylinConfig.getHdfsWorkingDirectory() + "/_backup");
        val rootMetadataFS = HadoopUtil.getFileSystem(rootMetadataPath);
        Assertions.assertThat(rootMetadataFS.listStatus(rootMetadataPath)).hasSize(1);

        val rootMetadataChildrenPath = rootMetadataFS.listStatus(rootMetadataPath)[0].getPath();
        Assertions.assertThat(rootMetadataFS.listStatus(rootMetadataChildrenPath)).hasSize(1).contains(rootMetadataFS
                .getFileStatus(new Path(rootMetadataChildrenPath.toString() + File.separator + "UUID")));

        rootMetadataFS.close();
    }

    @Test
    public void testCleanBeforeBackup() throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.env.hdfs-working-dir", temporaryFolder.getRoot().getAbsolutePath());
        val rootMetadataPath = new Path(kylinConfig.getHdfsWorkingDirectory() + "/_backup");

        val fs = HadoopUtil.getFileSystem(rootMetadataPath);
        fs.mkdirs(rootMetadataPath);

        int metadataBackupCountThreshold = kylinConfig.getMetadataBackupCountThreshold();
        for (int i = 0; i < metadataBackupCountThreshold - 1; i++) {
            fs.mkdirs(new Path(rootMetadataPath.toString() + "/test" + i));
        }
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(6);

        metadataBackupService.cleanBeforeBackup(kylinConfig);
        fs.mkdirs(new Path(rootMetadataPath.toString() + "/test" + (metadataBackupCountThreshold - 1)));
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(7);

        metadataBackupService.cleanBeforeBackup(kylinConfig);
        fs.mkdirs(new Path(rootMetadataPath.toString() + "/test" + metadataBackupCountThreshold));
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(7);

        kylinConfig.setProperty("kylin.metadata.backup-count-threshold", "3");
        metadataBackupService.cleanBeforeBackup(kylinConfig);
        Assertions.assertThat(fs.listStatus(rootMetadataPath)).hasSize(2);

        fs.close();
    }

}
