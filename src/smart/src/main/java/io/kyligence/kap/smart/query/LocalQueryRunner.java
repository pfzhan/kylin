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
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.smart.query.mockup.MockupPushDownRunner;
import lombok.val;

class LocalQueryRunner extends AbstractQueryRunner {

    private static final Logger logger = LoggerFactory.getLogger(LocalQueryRunner.class);
    private final Set<String> dumpResources;
    private final Map<String, RootPersistentEntity> mockupResources;

    LocalQueryRunner(KylinConfig srcKylinConfig, String projectName, String[] sqls, Set<String> dumpResources,
            Map<String, RootPersistentEntity> mockupResources, int threads) {

        super(projectName, sqls, threads);
        this.kylinConfig = srcKylinConfig;
        this.dumpResources = dumpResources;
        this.mockupResources = mockupResources;
    }

    @Override
    public KylinConfig prepareConfig() throws IOException {
        File tmp = File.createTempFile("kylin_job_meta", "");
        FileUtils.forceDelete(tmp);
        val properties = KylinConfig.getInstanceFromEnv().exportToProperties();
        properties.setProperty("kylin.metadata.url", tmp.getAbsolutePath());
        ResourceStore.dumpResources(KylinConfig.getInstanceFromEnv(), tmp, dumpResources, properties);

        for (Map.Entry<String, RootPersistentEntity> mockupResource : mockupResources.entrySet()) {
            File dumpFile = new File(tmp, mockupResource.getKey());
            File dumpParent = dumpFile.getParentFile();
            if (dumpParent.isFile()) {
                FileUtils.forceDelete(dumpParent);
            }
            FileUtils.forceMkdir(dumpParent);
            String dumpJson = JsonUtil.writeValueAsIndentString(mockupResource.getValue());
            FileUtils.writeStringToFile(dumpFile, dumpJson, Charset.defaultCharset());
        }

        KylinConfig config = KylinConfig.createKylinConfig(properties);
        config.setProperty("kylin.query.pushdown.runner-class-name", MockupPushDownRunner.class.getName());
        config.setProperty("kylin.query.pushdown-enabled", "true");
        config.setProperty("kylin.query.transformers", StringUtils.join(kylinConfig.getQueryTransformers(), ','));
        config.setProperty("kylin.query.security.acl-tcr-enabled", "false");

        return config;
    }

    @Override
    public void cleanupConfig(KylinConfig config) throws IOException {
        File metaDir = new File(config.getMetadataUrl().getIdentifier());
        if (metaDir.exists() && metaDir.isDirectory()) {
            FileUtils.forceDelete(metaDir);
            logger.debug("Deleted the meta dir: {}", metaDir);
        }

        ResourceStore.clearCache(config);
    }
}
