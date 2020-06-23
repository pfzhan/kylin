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

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SnapshotCleaner implements MetadataCleaner {

    private String project;
    private Set<String> staleDataFlowIds = new HashSet<>();
    private Set<String> staleSnapshotPaths = new HashSet<>();

    public SnapshotCleaner(String project) {
        this.project = project;
    }

    public void checkStaleSnapshots() {
        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (NDataflow dataFlow : dfMgr.listAllDataflows()) {
            for (NDataSegment segment : dataFlow.getSegments()) {
                Set<String> stalePaths = segment.getSnapshots().values().stream()
                        .filter(snapshotPath -> !snapshotExist(segment, snapshotPath)).collect(Collectors.toSet());
                if (!stalePaths.isEmpty()) {
                    staleDataFlowIds.add(dataFlow.getId());
                    staleSnapshotPaths.addAll(stalePaths);
                }
            }
        }
    }

    private boolean snapshotExist(NDataSegment segment, String snapshotPath) {
        if (staleSnapshotPaths.contains(snapshotPath)) {
            return false;
        }
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String baseDir = KapConfig.wrap(segment.getConfig()).getMetadataWorkingDirectory();
        String resourcePath = baseDir + "/" + snapshotPath;
        try {
            return fs.exists(new Path(resourcePath));
        } catch (IOException e) {
            return true; // on IOException, skip the checking
        }
    }

    @Override
    public void cleanup(String project) {
        // remove stale table snapshots from dataflow segments
        NDataflowManager dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (String staleDataFlowId : staleDataFlowIds) {
            dfMgr.updateDataflow(staleDataFlowId, copyForWrite -> {
                Segments<NDataSegment> segments = copyForWrite.getSegments();
                for (NDataSegment segment : segments) {

                    Map<String, String> validSnapshots = new HashMap<>();
                    for (Map.Entry<String, String> snapshot : segment.getSnapshots().entrySet()) {
                        if (staleSnapshotPaths.contains(snapshot.getValue())) {
                            continue;
                        }
                        validSnapshots.put(snapshot.getKey(), snapshot.getValue());
                    }

                    segment.setSnapshots(validSnapshots);
                }

                copyForWrite.setSegments(segments);
            });
        }

        // remove stale snapshot path from tables
        NTableMetadataManager tblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (TableDesc tableDesc : tblMgr.listAllTables()) {
            if (staleSnapshotPaths.contains(tableDesc.getLastSnapshotPath())) {
                TableDesc copy = tblMgr.copyForWrite(tableDesc);
                copy.setLastSnapshotPath(null);
                tblMgr.updateTableDesc(copy);
            }
        }
    }
}
