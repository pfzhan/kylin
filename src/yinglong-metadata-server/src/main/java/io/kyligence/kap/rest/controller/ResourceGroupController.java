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

package io.kyligence.kap.rest.controller;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.common.persistence.transaction.EpochCheckBroadcastNotifier;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroup;
import io.kyligence.kap.rest.handler.resourcegroup.IResourceGroupRequestValidator;
import io.kyligence.kap.rest.request.resourecegroup.ResourceGroupRequest;
import io.kyligence.kap.rest.service.resourcegroup.ResourceGroupService;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/resource_groups", produces = { HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class ResourceGroupController extends NBasicController {
    @Autowired
    private ResourceGroupService resourceGroupService;

    @Autowired
    private List<IResourceGroupRequestValidator> requestValidatorList;

    @ApiOperation(value = "resourceGroup", tags = { "SM" })
    @PutMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<String> updateResourceGroup(@RequestBody ResourceGroupRequest request) {
        checkResourceGroupRequest(request);

        if (!resourceGroupChanged(request)) {
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
        }

        resourceGroupService.updateResourceGroup(request);
        EventBusFactory.getInstance().postAsync(new EpochCheckBroadcastNotifier());

        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
    }

    private void checkResourceGroupRequest(ResourceGroupRequest request) {
        requestValidatorList.forEach(validator -> validator.validate(request));
    }

    private boolean resourceGroupChanged(ResourceGroupRequest request) {
        ResourceGroup originResourceGroup = resourceGroupService.getResourceGroup();
        if (!request.isResourceGroupEnabled() && !originResourceGroup.isResourceGroupEnabled()) {
            return false;
        }
        if (request.isResourceGroupEnabled() != originResourceGroup.isResourceGroupEnabled()) {
            return true;
        }
        if (checkListChanged(request.getResourceGroupEntities(), originResourceGroup.getResourceGroupEntities())) {
            return true;
        }
        if (checkListChanged(request.getKylinInstances(), originResourceGroup.getKylinInstances())) {
            return true;
        }
        return checkListChanged(request.getResourceGroupMappingInfoList(),
                originResourceGroup.getResourceGroupMappingInfoList());
    }

    private boolean checkListChanged(Collection<?> list1, Collection<?> list2) {
        return !CollectionUtils.isEqualCollection(list1, list2);
    }
}