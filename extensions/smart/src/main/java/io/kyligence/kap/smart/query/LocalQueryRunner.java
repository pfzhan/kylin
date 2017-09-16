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
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;

class LocalQueryRunner extends AbstractQueryRunner {
    final private Set<String> dumpResources;
    final private Map<String, RootPersistentEntity> mockupResources;

    LocalQueryRunner(String projectName, String[] sqls, Set<String> dumpResources,
            Map<String, RootPersistentEntity> mockupResources, int threads) {
        super(projectName, sqls, threads);

        this.dumpResources = dumpResources;
        this.mockupResources = mockupResources;
    }

    @Override
    public KylinConfig prepareConfig() throws Exception {
        String metaPath = ResourceStore.dumpResources(KylinConfig.getInstanceFromEnv(), dumpResources);
        File metaDir = new File(new URI(metaPath));

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
        return config;
    }

    @Override
    public void cleanupConfig(KylinConfig config) throws Exception {
        Utils.clearCacheForKylinConfig(config);

        File metaDir = new File(config.getMetadataUrl().toString());
        if (metaDir.exists() && metaDir.isDirectory()) {
            FileUtils.forceDelete(metaDir);
        }
    }
}
