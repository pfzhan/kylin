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

package io.kyligence.kap.rest.service.resourcegroup;

import io.kyligence.kap.metadata.resourcegroup.ResourceGroupManager;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.kyligence.kap.metadata.resourcegroup.ResourceGroup;
import io.kyligence.kap.rest.request.resourecegroup.ResourceGroupRequest;
import io.kyligence.kap.rest.aspect.Transaction;
import lombok.val;

@Service("resourceGroupService")
public class ResourceGroupService extends BasicService {

    @Autowired
    public AclEvaluate aclEvaluate;

    @Transaction
    public void updateResourceGroup(ResourceGroupRequest request) {
        aclEvaluate.checkIsGlobalAdmin();
        val manager = getManager(ResourceGroupManager.class);
        manager.updateResourceGroup(copyForWrite -> {
            copyForWrite.setResourceGroupEnabled(request.isResourceGroupEnabled());
            copyForWrite.setResourceGroupEntities(request.getResourceGroupEntities());
            copyForWrite.setKylinInstances(request.getKylinInstances());
            copyForWrite.setResourceGroupMappingInfoList(request.getResourceGroupMappingInfoList());
        });
    }

    public ResourceGroup getResourceGroup() {
        val manager = getManager(ResourceGroupManager.class);
        return manager.getResourceGroup();
    }
}
