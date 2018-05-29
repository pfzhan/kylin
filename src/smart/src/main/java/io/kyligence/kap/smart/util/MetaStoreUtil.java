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
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;

public class MetaStoreUtil {
    private static final Logger logger = LoggerFactory.getLogger(MetaStoreUtil.class);

    public static File prepareLocalMetaStore(String projName, NCubePlan cubePlan)
            throws IOException, URISyntaxException {
        NDataflow dataflow = NDataflow.create(cubePlan.getName(), cubePlan);
        dataflow.setStatus(RealizationStatusEnum.READY);

        NDataModel modelDesc = cubePlan.getModel();
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(projName);
        projectInstance.init();

        projectInstance.addModel(modelDesc.getName());
        projectInstance.addRealizationEntry(dataflow.getType(), dataflow.getName());

        Set<String> dumpResources = Sets.newHashSet();
        dumpResources.add(modelDesc.getResourcePath());
        for (TableRef tableRef : modelDesc.getAllTables()) {
            dumpResources.add(tableRef.getTableDesc().getResourcePath());
            projectInstance.addTable(tableRef.getTableIdentity());
        }

        String metaPath = dumpResources(KylinConfig.getInstanceFromEnv(), dumpResources);
        File metaDir = new File(new URI(metaPath));
        FileUtils.writeStringToFile(new File(metaDir, dataflow.getResourcePath()),
                JsonUtil.writeValueAsIndentString(dataflow), Charset.defaultCharset());
        FileUtils.writeStringToFile(new File(metaDir, cubePlan.getResourcePath()),
                JsonUtil.writeValueAsIndentString(cubePlan), Charset.defaultCharset());
        FileUtils.writeStringToFile(new File(metaDir, projectInstance.getResourcePath()),
                JsonUtil.writeValueAsIndentString(projectInstance), Charset.defaultCharset());

        return metaDir;
    }

    public static File prepareLocalMetaStore(String projName, List<TableDesc> tables)
            throws IOException, URISyntaxException {
        ProjectInstance projectInstance = new ProjectInstance();
        projectInstance.setName(projName);
        projectInstance.init();

        Set<String> dumpResources = Sets.newHashSet();
        for (TableDesc tableDesc : tables) {
            dumpResources.add(tableDesc.getResourcePath());
            projectInstance.addTable(tableDesc.getIdentity());
        }

        String metaPath = dumpResources(KylinConfig.getInstanceFromEnv(), dumpResources);
        File metaDir = new File(new URI(metaPath));
        FileUtils.writeStringToFile(new File(metaDir, projectInstance.getResourcePath()),
                JsonUtil.writeValueAsIndentString(projectInstance), Charset.defaultCharset());

        return metaDir;
    }
    
    public static String dumpResources(KylinConfig kylinConfig, Collection<String> dumpList) throws IOException {
        File tmp = File.createTempFile("kylin_job_meta", "");
        FileUtils.forceDelete(tmp); // we need a directory, so delete the file first

        File metaDir = new File(tmp, "meta");
        metaDir.mkdirs();

        // write kylin.properties
        File kylinPropsFile = new File(metaDir, "kylin.properties");
        kylinConfig.exportToFile(kylinPropsFile);

        ResourceStore from = ResourceStore.getKylinMetaStore(kylinConfig);
        KylinConfig localConfig = KylinConfig.createInstanceFromUri(metaDir.getAbsolutePath());
        ResourceStore to = ResourceStore.getKylinMetaStore(localConfig);
        for (String path : dumpList) {
            RawResource res = null;
            res = from.getResource(path);
            if (res == null)
                throw new IllegalStateException("No resource found at -- " + path);
            to.putResource(path, res.inputStream, res.timestamp);
            res.inputStream.close();
        }

        String metaDirURI = OptionsHelper.convertToFileURL(metaDir.getAbsolutePath());
        if (metaDirURI.startsWith("/")) // note Path on windows is like "d:/../..."
            metaDirURI = "file://" + metaDirURI;
        else
            metaDirURI = "file:///" + metaDirURI;
        logger.info("meta dir is: " + metaDirURI);
        
        return metaDirURI;
    }
}
