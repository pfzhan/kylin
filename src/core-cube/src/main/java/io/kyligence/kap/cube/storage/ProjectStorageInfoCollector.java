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

package io.kyligence.kap.cube.storage;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class ProjectStorageInfoCollector {

    private List<StorageInfoCollector> collectors = Lists.newArrayList();

    private static final ImmutableMap<Class, StorageInfoEnum> collectorType = ImmutableMap
            .<Class, StorageInfoEnum> builder().put(GarbageStorageCollector.class, StorageInfoEnum.GARBAGE_STORAGE)
            .put(TotalStorageCollector.class, StorageInfoEnum.TOTAL_STORAGE)
            .put(StorageQuotaCollector.class, StorageInfoEnum.STORAGE_QUOTA).build();

    public ProjectStorageInfoCollector(List<StorageInfoEnum> storageInfoList) {
        if (CollectionUtils.isNotEmpty(storageInfoList)) {
            storageInfoList.forEach(si -> addCollectors(si));
        }
    }

    private void collect(KylinConfig config, String project, StorageVolumeInfo storageVolumeInfo) {
        for (StorageInfoCollector collector : collectors) {
            try {
                collector.collect(config, project, storageVolumeInfo);
            } catch (Exception e) {
                storageVolumeInfo.getThrowableMap().put(collectorType.get(collector.getClass()), e);
            }
        }
    }

    private void addCollectors(StorageInfoEnum storageInfoEnum) {
        switch (storageInfoEnum) {
        case GARBAGE_STORAGE:
            collectors.add(new GarbageStorageCollector());
            break;
        case TOTAL_STORAGE:
            collectors.add(new TotalStorageCollector());
            break;
        case STORAGE_QUOTA:
            collectors.add(new StorageQuotaCollector());
            break;
        default:
            break;
        }
    }

    public StorageVolumeInfo getStorageVolumeInfo(KylinConfig config, String project) {
        StorageVolumeInfo storageVolumeInfo = new StorageVolumeInfo();
        if (StringUtils.isBlank(project) || CollectionUtils.isEmpty(collectors)) {
            return storageVolumeInfo;
        }
        collect(config, project, storageVolumeInfo);
        return storageVolumeInfo;
    }
}
