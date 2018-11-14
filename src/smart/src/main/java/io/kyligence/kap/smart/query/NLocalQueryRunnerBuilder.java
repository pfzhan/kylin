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

import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;

class NLocalQueryRunnerBuilder {

    private KylinConfig srcKylinConfig;
    private String[] sqls;
    private int nThreads;

    NLocalQueryRunnerBuilder(KylinConfig srcKylinConfig, String[] sqls, int nThreads) {
        this.srcKylinConfig = srcKylinConfig;
        this.sqls = sqls;
        this.nThreads = nThreads;
    }

    NLocalQueryRunner buildBasic(String projectName) {
        NTableMetadataManager metadataManager = NTableMetadataManager.getInstance(srcKylinConfig, projectName);
        NProjectManager projectManager = NProjectManager.getInstance(srcKylinConfig);
        ProjectInstance srcProj = projectManager.getProject(projectName);

        ProjectInstance dumpProj = new ProjectInstance();
        dumpProj.setName(projectName);
        dumpProj.setTables(srcProj.getTables());
        dumpProj.init();

        Map<String, RootPersistentEntity> mockupResources = Maps.newHashMap();
        mockupResources.put(dumpProj.getResourcePath(), dumpProj);

        Set<String> dumpResources = Sets.newHashSet();
        for (TableDesc tableDesc : metadataManager.listAllTables()) {
            dumpResources.add(tableDesc.getResourcePath());
        }

        return new NLocalQueryRunner(srcKylinConfig, projectName, sqls, dumpResources, mockupResources, nThreads);
    }
}
