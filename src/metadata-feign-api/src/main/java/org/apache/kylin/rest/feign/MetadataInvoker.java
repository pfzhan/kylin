/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.feign;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.HDFSMetadataStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.rest.util.SpringContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class MetadataInvoker {

    @Autowired(required = false)
    private MetadataRPC delegate;

    public static MetadataInvoker getInstance() {
        MetadataStore metadataStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getMetadataStore();
        if (metadataStore instanceof HDFSMetadataStore) {
            throw new KylinRuntimeException("This request cannot be route to metadata server");
        }
        if (SpringContext.getApplicationContext() == null) {
            // for UT
            return new MetadataInvoker();
        } else {
            return SpringContext.getBean(MetadataInvoker.class);
        }
    }

    private MetadataContract getDelegate() {
        if (delegate == null) {
            // for UT
            try {
                return ClassUtil.forName("org.apache.kylin.rest.service.ModelService", MetadataContract.class)
                        .newInstance();
            } catch (Exception ignored) {
            }
        }
        return delegate;
    }

    public List<NDataLayout[]> mergeMetadata(String project, MergerInfo mergerInfo) {
        return getDelegate().mergeMetadata(project, mergerInfo);
    }

    public void makeSegmentReady(String project, String modelId, String segmentId, int errorOrPausedJobCount) {
        getDelegate().makeSegmentReady(project, modelId, segmentId, errorOrPausedJobCount);
    }

    static class MockedDelegate implements MetadataContract {

        @Override
        public List<NDataLayout[]> mergeMetadata(String project, MergerInfo mergerInfo) {
            return new ArrayList<>();
        }

        @Override
        public void makeSegmentReady(String project, String modelId, String segmentId, int errorOrPausedJobCount) {

        }
    }

}
