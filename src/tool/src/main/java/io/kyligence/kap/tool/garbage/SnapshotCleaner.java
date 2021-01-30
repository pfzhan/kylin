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

package io.kyligence.kap.tool.garbage;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.model.NTableMetadataManager;

public class SnapshotCleaner implements MetadataCleaner {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotCleaner.class);

    private String project;
    private Set<String> staleSnapshotPaths = new HashSet<>();

    public SnapshotCleaner(String project) {
        this.project = project;
    }

    public void checkStaleSnapshots() {
        NTableMetadataManager tMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        tMgr.listAllTables().forEach(tableDesc -> {
            String snapshotPath = tableDesc.getLastSnapshotPath();
            if (snapshotPath != null && !snapshotExist(snapshotPath, KapConfig.getInstanceFromEnv())) {
                staleSnapshotPaths.add(snapshotPath);
            }
        });
    }

    private boolean snapshotExist(String snapshotPath, KapConfig config) {
        if (staleSnapshotPaths.contains(snapshotPath)) {
            return false;
        }
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String baseDir = config.getMetadataWorkingDirectory();
        String resourcePath = baseDir + "/" + snapshotPath;
        try {
            return fs.exists(new Path(resourcePath));
        } catch (IOException e) {
            return true; // on IOException, skip the checking
        }
    }

    @Override
    public void cleanup(String project) {
        logger.info("Start to clean snapshot in project {}", project);
        // remove stale snapshot path from tables
        NTableMetadataManager tblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (TableDesc tableDesc : tblMgr.listAllTables()) {
            if (staleSnapshotPaths.contains(tableDesc.getLastSnapshotPath())) {
                TableDesc copy = tblMgr.copyForWrite(tableDesc);
                copy.deleteSnapshot();

                TableExtDesc ext = tblMgr.getOrCreateTableExt(tableDesc);
                TableExtDesc extCopy = tblMgr.copyForWrite(ext);
                extCopy.setOriginalSize(-1);

                tblMgr.mergeAndUpdateTableExt(ext, extCopy);
                tblMgr.updateTableDesc(copy);
            }
        }
        logger.info("Clean snapshot in project {} finished", project);
    }
}
