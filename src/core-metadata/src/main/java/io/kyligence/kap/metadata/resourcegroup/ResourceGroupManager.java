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

package io.kyligence.kap.metadata.resourcegroup;

import java.util.List;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResourceGroupManager {
    private KylinConfig config;
    private CachedCrudAssist<ResourceGroup> crud;

    public static ResourceGroupManager getInstance(KylinConfig config) {
        return config.getManager(ResourceGroupManager.class);
    }

    // called by reflection
    static ResourceGroupManager newInstance(KylinConfig config) {
        return new ResourceGroupManager(config);
    }

    private ResourceGroupManager(KylinConfig cfg) {
        this.config = cfg;
        crud = new CachedCrudAssist<ResourceGroup>(getStore(), ResourceStore.RESOURCE_GROUP, ResourceGroup.class) {
            @Override
            protected ResourceGroup initEntityAfterReload(ResourceGroup entity, String resourceName) {
                return entity;
            }
        };
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    /**
     * @return There is only one resource group metadata.
     */
    public ResourceGroup getResourceGroup() {
        List<ResourceGroup> resourceGroup = crud.listAll();
        if (CollectionUtils.isEmpty(resourceGroup)) {
            return null;
        }
        return resourceGroup.get(0);
    }

    public boolean resourceGroupInitialized() {
        return CollectionUtils.isNotEmpty(crud.listAll());
    }

    public boolean isProjectBindToResourceGroup(String project) {
        if (UnitOfWork.GLOBAL_UNIT.equals(project)) {
            return true;
        }
        return getResourceGroup().getResourceGroupMappingInfoList()
                .stream()
                .anyMatch(mapping -> project.equals(mapping.getProject()));
    }

    public boolean isResourceGroupEnabled() {
        // resource group metadata not exist
        if (!resourceGroupInitialized()) {
            return false;
        }
        return getResourceGroup().isResourceGroupEnabled();
    }

    public void initResourceGroup() {
        if (!resourceGroupInitialized()) {
            save(new ResourceGroup());
        }
    }

    public interface ResourceGroupUpdater {
        void modify(ResourceGroup copyForWrite);
    }

    public ResourceGroup updateResourceGroup(ResourceGroupUpdater updater) {
        ResourceGroup cached = getResourceGroup();
        ResourceGroup copy = copyForWrite(cached);
        updater.modify(copy);
        return updateResourceGroup(copy);
    }

    public boolean instanceHasPermissionToOwnEpochTarget(String epochTarget, String server) {
        if (UnitOfWork.GLOBAL_UNIT.equals(epochTarget) || !isResourceGroupEnabled()) {
            return true;
        }
        // when resource group enabled, project owner must be in the build resource group
        ResourceGroup resourceGroup = getResourceGroup();
        String epochServerResourceGroupId = resourceGroup.getKylinInstances()
                .stream()
                .filter(instance -> instance.getInstance().equals(server))
                .map(KylinInstance::getResourceGroupId)
                .findFirst().orElse(null);
        return resourceGroup.getResourceGroupMappingInfoList()
                .stream()
                .filter(mappingInfo -> mappingInfo.getProject().equals(epochTarget))
                .filter(mappingInfo -> mappingInfo.getRequestType() == RequestTypeEnum.BUILD)
                .anyMatch(mappingInfo -> mappingInfo.getResourceGroupId().equals(epochServerResourceGroupId));
    }

    private ResourceGroup copyForWrite(ResourceGroup resourceGroup) {
        Preconditions.checkNotNull(resourceGroup);
        return crud.copyForWrite(resourceGroup);
    }

    private ResourceGroup updateResourceGroup(ResourceGroup resourceGroup) {
        if (!crud.contains(ResourceGroup.RESOURCE_GROUP)) {
            throw new IllegalArgumentException("Resource Group metadata does not exist!");
        }
        return save(resourceGroup);
    }

    private ResourceGroup save(ResourceGroup resourceGroup) {
        crud.save(resourceGroup);
        return resourceGroup;
    }
}
