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

package io.kyligence.kap.smart.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;

import com.google.common.collect.Sets;

public class MetaStoreUtil {
    public static File prepareLocalMetaStore(String projName, CubeDesc cubeDesc) throws IOException, URISyntaxException {
        CubeInstance cubeInstance = CubeInstance.create(cubeDesc.getName(), cubeDesc);
        cubeInstance.setStatus(RealizationStatusEnum.READY);

        DataModelDesc modelDesc = cubeDesc.getModel();
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(projName);
        projectInstance.init();

        projectInstance.addModel(modelDesc.getName());
        projectInstance.addRealizationEntry(RealizationType.CUBE, cubeInstance.getName());

        Set<String> dumpResources = Sets.newHashSet();
        dumpResources.add(modelDesc.getResourcePath());
        for (TableRef tableRef : modelDesc.getAllTables()) {
            dumpResources.add(tableRef.getTableDesc().getResourcePath());
            projectInstance.addTable(tableRef.getTableIdentity());
        }

        String metaPath = ResourceStore.dumpResources(KylinConfig.getInstanceFromEnv(), dumpResources);
        File metaDir = new File(new URI(metaPath));
        FileUtils.writeStringToFile(new File(metaDir, cubeInstance.getResourcePath()),
                JsonUtil.writeValueAsIndentString(cubeInstance), Charset.defaultCharset());
        FileUtils.writeStringToFile(new File(metaDir, cubeDesc.getResourcePath()),
                JsonUtil.writeValueAsIndentString(cubeDesc), Charset.defaultCharset());
        FileUtils.writeStringToFile(new File(metaDir, projectInstance.getResourcePath()),
                JsonUtil.writeValueAsIndentString(projectInstance), Charset.defaultCharset());

        return metaDir;
    }

    public static File prepareLocalMetaStore(String projName, List<TableDesc> tables) throws IOException, URISyntaxException {
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(projName);
        projectInstance.init();

        Set<String> dumpResources = Sets.newHashSet();
        for (TableDesc tableDesc : tables) {
            dumpResources.add(tableDesc.getResourcePath());
            projectInstance.addTable(tableDesc.getIdentity());
        }

        String metaPath = ResourceStore.dumpResources(KylinConfig.getInstanceFromEnv(), dumpResources);
        File metaDir = new File(new URI(metaPath));
        FileUtils.writeStringToFile(new File(metaDir, projectInstance.getResourcePath()),
                JsonUtil.writeValueAsIndentString(projectInstance), Charset.defaultCharset());

        return metaDir;
    }
}
