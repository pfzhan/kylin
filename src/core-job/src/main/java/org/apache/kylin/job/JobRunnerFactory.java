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
package org.apache.kylin.job;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWorkParams;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobRunnerFactory {

    private JobRunnerFactory() {
        // Just implement it
    }

    public static AbstractJobRunner createRunner(KylinConfig config, String type, String project,
            List<String> resources) {
        switch (type) {
        case "fork":
            return new ForkBasedJobRunner(config, project, resources);
        case "in-memory":
            return new InMemoryJobRunner(config, project, resources);
        default:
            throw new NotImplementedException("Runner type " + type + " not implement");
        }
    }

    @RequiredArgsConstructor
    public abstract static class AbstractJobRunner {

        protected final KylinConfig kylinConfig;
        protected final String project;
        protected final List<String> originResources;

        protected String metaDumpUrl;
        protected String jobId;

        public void init(String jobId) {
            this.jobId = jobId;
            val jobTmpDir = getJobTmpDir();
            metaDumpUrl = kylinConfig.getMetadataUrlPrefix() + "@hdfs,path=file://" + jobTmpDir + "/meta";
        }

        public void start(ExecutableApplication app, Map<String, String> args) throws Exception {
            attachMetadataAndKylinProps(false);
            args.put("meta", metaDumpUrl);
            args.put("metaOutput", metaDumpUrl + "_output");
            doExecute(app, args);
        }

        public void cleanupEnv() {
            FileUtils.deleteQuietly(new File(getJobTmpDir()));
        }

        protected abstract void doExecute(ExecutableApplication app, Map<String, String> args) throws Exception;

        protected String formatArgs(Map<String, String> args) {
            return args.entrySet().stream().map(entry -> "--" + entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.joining(" "));
        }

        public String getJobTmpDir() {
            return KylinConfigBase.getKylinHome() + "/tmp/" + jobId;
        }

        protected void attachMetadataAndKylinProps(boolean kylinPropsOnly) throws IOException {
            if (StringUtils.isEmpty(metaDumpUrl)) {
                throw new RuntimeException("Missing metaUrl");
            }

            File tmpDir = File.createTempFile("kylin_job_meta", "");
            try {
                org.apache.commons.io.FileUtils.forceDelete(tmpDir); // we need a directory, so delete the file first

                Properties props = kylinConfig.exportToProperties();
                props.setProperty("kylin.query.queryhistory.url", kylinConfig.getMetadataUrl().toString());
                props.setProperty("kylin.metadata.url", metaDumpUrl);

                if (kylinPropsOnly) {
                    ResourceStore.dumpKylinProps(tmpDir, props);
                } else {
                    // The way of Updating metadata is CopyOnWrite. So it is safe to use Reference in the value.
                    Map<String, RawResource> dumpMap = EnhancedUnitOfWork
                            .doInTransactionWithCheckAndRetry(UnitOfWorkParams.<Map> builder().readonly(true)
                                    .unitName(project).maxRetry(1).processor(() -> {
                                        Map<String, RawResource> retMap = Maps.newHashMap();
                                        for (String resPath : originResources) {
                                            ResourceStore resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
                                            RawResource rawResource = resourceStore.getResource(resPath);
                                            retMap.put(resPath, rawResource);
                                        }
                                        return retMap;
                                    }).build());

                    if (Objects.isNull(dumpMap) || dumpMap.isEmpty()) {
                        return;
                    }
                    // dump metadata
                    ResourceStore.dumpResourceMaps(kylinConfig, tmpDir, dumpMap, props);
                }

                // copy metadata to target metaUrl
                KylinConfig dstConfig = KylinConfig.createKylinConfig(props);
                MetadataStore.createMetadataStore(dstConfig).uploadFromFile(tmpDir);
                // clean up
                log.debug("Copied metadata to the target metaUrl, delete the temp dir: {}", tmpDir);
            } finally {
                FileUtils.forceDelete(tmpDir);
            }
        }
    }
}
