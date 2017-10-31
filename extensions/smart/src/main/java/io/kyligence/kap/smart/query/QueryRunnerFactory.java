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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.cube.domain.ModelDomainBuilder;

public final class QueryRunnerFactory {

    public static AbstractQueryRunner createForCubeSuggestion(KylinConfig srcKylinConfig, String[] sqls, int nThreads,
            String projectName) {
        DataModelManager metadataManager = DataModelManager.getInstance(srcKylinConfig);
        List<DataModelDesc> modelDescs = metadataManager.getModels(projectName);
        List<CubeDesc> mockupCubes = Lists.newArrayListWithExpectedSize(modelDescs.size());

        KylinConfig config = KylinConfig.createKylinConfig(srcKylinConfig);
        for (DataModelDesc m : modelDescs) {
            CubeDesc mockupCube = new ModelDomainBuilder(m).build().buildCubeDesc();
            mockupCubes.add(mockupCube);
        }

        return new LocalQueryRunnerBuilder(srcKylinConfig, sqls, nThreads).buildWithCubeDescs(projectName, mockupCubes);
    }

    public static AbstractQueryRunner createForModelSuggestion(KylinConfig srcKylinConfig, String[] sqls, int nThreads,
            String projectName) {
        return new LocalQueryRunnerBuilder(srcKylinConfig, sqls, nThreads).buildWithCubeDescs(projectName,
                Lists.newArrayList(new CubeDesc[0]));
    }

    public static AbstractQueryRunner createForCubeSQLValid(KylinConfig srcKylinConfig, String[] sqls, int nThreads,
            CubeDesc cubeDesc) {
        ProjectManager projectManager = ProjectManager.getInstance(srcKylinConfig);
        CubeDescManager cubeDescManager = CubeDescManager.getInstance(srcKylinConfig);
        List<CubeDesc> cubeDescs = Lists.newArrayList();
        for (RealizationEntry entry : projectManager.getProject(cubeDesc.getProject()).getRealizationEntries()) {
            if (entry.getType().equals(CubeInstance.REALIZATION_TYPE)) {
                CubeDesc c = cubeDescManager.getCubeDesc(entry.getRealization());
                if (c != null)
                    cubeDescs.add(c);
            }
        }

        return new LocalQueryRunnerBuilder(srcKylinConfig, sqls, nThreads).buildWithCubeDescs(cubeDesc.getProject(),
                cubeDescs);
    }

    public static AbstractQueryRunner createForModelSQLValid(KylinConfig srcKylinConfig, String[] sqls, int nThreads,
            DataModelDesc modelDesc) {
        DataModelManager metadataManager = DataModelManager.getInstance(srcKylinConfig);
        List<DataModelDesc> modelDescs = metadataManager.getModels(modelDesc.getProject());
        List<CubeDesc> mockupCubes = Lists.newArrayListWithExpectedSize(modelDescs.size());

        KylinConfig config = KylinConfig.createKylinConfig(srcKylinConfig);
        config.setProperty("kap.smart.conf.measure.query-enabled", "false");
        for (DataModelDesc m : modelDescs) {
            CubeDesc mockupCube = new ModelDomainBuilder(m).build().buildCubeDesc();
            mockupCubes.add(mockupCube);
        }

        return new LocalQueryRunnerBuilder(srcKylinConfig, sqls, nThreads).buildWithCubeDescs(modelDesc.getProject(),
                mockupCubes);
    }

}
