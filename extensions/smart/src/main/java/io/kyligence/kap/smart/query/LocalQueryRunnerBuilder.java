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
package io.kyligence.kap.smart.query;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

class LocalQueryRunnerBuilder {
    private KylinConfig srcKylinConfig;
    private String[] sqls;
    private int nThreads;

    LocalQueryRunnerBuilder(KylinConfig srcKylinConfig, String[] sqls, int nThreads) {
        this.srcKylinConfig = srcKylinConfig;
        this.sqls = sqls;
        this.nThreads = nThreads;
    }

    LocalQueryRunner buildWithCubeDescs(String projectName, List<CubeDesc> cubeDescs) {
        Set<String> dumpResources = Sets.newHashSet();
        Map<String, RootPersistentEntity> mockupResources = Maps.newHashMap();

        CubeManager cubeManager = CubeManager.getInstance(srcKylinConfig);
        TableMetadataManager metadataManager = TableMetadataManager.getInstance(srcKylinConfig);
        ProjectManager projectManager = ProjectManager.getInstance(srcKylinConfig);

        ProjectInstance srcProj = projectManager.getProject(projectName);

        ProjectInstance dumpProj = new ProjectInstance();
        dumpProj.setName(projectName);
        dumpProj.setTables(srcProj.getTables());
        dumpProj.init();
        mockupResources.put(dumpProj.getResourcePath(), dumpProj);

        for (TableDesc tableDesc : metadataManager.listAllTables(projectName)) {
            dumpResources.add(tableDesc.getResourcePath());
        }

        for (CubeDesc cubeDesc : cubeDescs) {
            DataModelDesc modelDesc = cubeDesc.getModel();
            dumpResources.add(modelDesc.getResourcePath());
            if (!dumpProj.containsModel(modelDesc.getName()))
                dumpProj.addModel(modelDesc.getName());

            CubeInstance cubeInstance = cubeManager.getCube(cubeDesc.getName());
            if (cubeInstance == null) {
                // mockup cube_desc
                cubeInstance = CubeInstance.create(cubeDesc.getName(), cubeDesc);
                mockupResources.put(cubeDesc.getResourcePath(), cubeDesc);
            } else {
                cubeInstance = CubeInstance.getCopyOf(cubeInstance);
                dumpResources.add(cubeDesc.getResourcePath());
            }

            if (cubeInstance.getSegments().isEmpty()) {
                CubeSegment mockSeg = new CubeSegment();
                mockSeg.setUuid(UUID.randomUUID().toString());
                mockSeg.setStorageLocationIdentifier(UUID.randomUUID().toString());
                ConcurrentHashMap<String, String> snapshots = new ConcurrentHashMap<>();
                for (TableRef tblRef : modelDesc.getLookupTables()) {
                    snapshots.put(tblRef.getTableIdentity(), "");
                }
                mockSeg.setSnapshots(snapshots);
                mockSeg.setStatus(SegmentStatusEnum.READY);
                cubeInstance.getSegments().add(mockSeg);
            }
            cubeInstance.setStatus(RealizationStatusEnum.READY);
            dumpProj.addRealizationEntry(RealizationType.CUBE, cubeInstance.getName());
            mockupResources.put(cubeInstance.getResourcePath(), cubeInstance);
        }

        return new LocalQueryRunner(srcKylinConfig, projectName, sqls, dumpResources, mockupResources, nThreads);
    }
}
