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

package io.kyligence.kap.common.persistence;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.FederatedResourceStore;
import org.apache.kylin.common.persistence.HDFSResourceStore;
import org.apache.kylin.common.persistence.ResourceStore;

import com.google.common.collect.ImmutableMap;;

/**
 * Keep dictionary and snapshot in separated HDFSResourceStore
 */
public class KapMetaStoreFactory implements ResourceStore.IKylinMetaStoreFactory {

    @Override
    public ResourceStore createMetaStore(KylinConfig config) {
        try {
            ResourceStore base = ResourceStore.createResourceStore(config, config.getMetadataUrl());

            KapConfig kapConf = KapConfig.wrap(config);
            StorageURL hdfsUrl = new StorageURL(config.getMetadataUrl().getIdentifier() + "-dict", //
                    HDFSResourceStore.HDFS_SCHEME, //
                    ImmutableMap.of("path", kapConf.getReadHdfsWorkingDirectory() + "dict-store"));
            HDFSResourceStore dictStore = new HDFSResourceStore(config, hdfsUrl);

            Map<String, ResourceStore> delegates = new HashMap<>();
            delegates.put(ResourceStore.DICT_RESOURCE_ROOT, dictStore);
            delegates.put(ResourceStore.SNAPSHOT_RESOURCE_ROOT, dictStore);
            
            return new FederatedResourceStore(base, delegates);
            
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
