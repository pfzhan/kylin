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

import java.util.Collections;
import java.util.List;

import lombok.EqualsAndHashCode;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import lombok.Setter;

@Setter
@EqualsAndHashCode(callSuper = false)
public class ResourceGroup extends RootPersistentEntity {
    @JsonProperty("resource_group_enabled")
    private boolean resourceGroupEnabled = false;

    @JsonProperty("resource_groups")
    private List<ResourceGroupEntity> resourceGroupEntities = Lists.newArrayList();

    @JsonProperty("instances")
    private List<KylinInstance> kylinInstances = Lists.newArrayList();

    @JsonProperty("mapping_info")
    private List<ResourceGroupMappingInfo> resourceGroupMappingInfoList = Lists.newArrayList();

    public static final String RESOURCE_GROUP = "relation";

    @Override
    public String resourceName() {
        return RESOURCE_GROUP;
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(RESOURCE_GROUP);
    }

    public static String concatResourcePath(String name) {
        return ResourceStore.RESOURCE_GROUP + "/" + name + MetadataConstants.FILE_SURFIX;
    }

    public boolean isResourceGroupEnabled() {
        return resourceGroupEnabled;
    }

    public List<ResourceGroupEntity> getResourceGroupEntities() {
        return Collections.unmodifiableList(resourceGroupEntities);
    }

    public List<KylinInstance> getKylinInstances() {
        return Collections.unmodifiableList(kylinInstances);
    }

    public List<ResourceGroupMappingInfo> getResourceGroupMappingInfoList() {
        return Collections.unmodifiableList(resourceGroupMappingInfoList);
    }
}
