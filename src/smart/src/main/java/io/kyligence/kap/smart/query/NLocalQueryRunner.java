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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;

import io.kyligence.kap.smart.util.MetaStoreUtil;

class NLocalQueryRunner extends AbstractQueryRunner {

    private final Set<String> dumpResources;
    private final Map<String, RootPersistentEntity> mockupResources;

    NLocalQueryRunner(KylinConfig srcKylinConfig, String projectName, String[] sqls, Set<String> dumpResources,
            Map<String, RootPersistentEntity> mockupResources, int threads) {

        super(projectName, sqls, threads);
        this.kylinConfig = srcKylinConfig;
        this.dumpResources = dumpResources;
        this.mockupResources = mockupResources;
    }

    @Override
    public KylinConfig prepareConfig() throws IOException {
        String metaPath = MetaStoreUtil.dumpResources(KylinConfig.getInstanceFromEnv(), dumpResources);
        File metaDir;
        try {
            metaDir = new File(new URI(metaPath));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }

        for (Map.Entry<String, RootPersistentEntity> mockupResource : mockupResources.entrySet()) {
            File dumpFile = new File(metaDir, mockupResource.getKey());
            File dumpParent = dumpFile.getParentFile();
            if (dumpParent.isFile()) {
                FileUtils.forceDelete(dumpParent);
            }
            FileUtils.forceMkdir(dumpParent);
            String dumpJson = JsonUtil.writeValueAsIndentString(mockupResource.getValue());
            FileUtils.writeStringToFile(dumpFile, dumpJson, Charset.defaultCharset());
        }

        KylinConfig config = Utils.newKylinConfig(metaDir.getAbsolutePath());
        Utils.exposeAllTableAndColumn(config);
        Utils.setLargeCuboidCombinationConf(config);
        Utils.setLargeRowkeySizeConf(config);
        config.setProperty("kylin.query.disable-cube-noagg-sql", Boolean.toString(kylinConfig.isDisableCubeNoAggSQL()));
        config.setProperty("kylin.query.transformers", StringUtils.join(kylinConfig.getQueryTransformers(), ','));
        config.setProperty("kap.query.security.row-acl-enabled", "false");
        config.setProperty("kap.query.security.column-acl-enabled", "false");

        config.setProperty("kylin.metadata.data-model-impl", "io.kyligence.kap.metadata.model.NDataModel");
        config.setProperty("kylin.metadata.data-model-manager-impl",
                "io.kyligence.kap.metadata.model.NDataModelManager");
        config.setProperty("kylin.metadata.project-manager-impl", "io.kyligence.kap.metadata.project.NProjectManager");
        config.setProperty("kylin.metadata.realization-providers", "io.kyligence.kap.cube.model.NDataflowManager");
        return config;
    }

    @Override
    public void cleanupConfig(KylinConfig config) throws IOException {
        Utils.clearCacheForKylinConfig(config);
        File metaDir = new File(config.getMetadataUrl().toString());
        if (metaDir.exists() && metaDir.isDirectory()) {
            FileUtils.forceDelete(metaDir);
        }
        ResourceStore.clearCache(config);
    }
}
