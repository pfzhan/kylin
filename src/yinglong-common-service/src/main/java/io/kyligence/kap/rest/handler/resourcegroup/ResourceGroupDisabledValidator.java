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

package io.kyligence.kap.rest.handler.resourcegroup;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.resourcegroup.ResourceGroupManager;
import io.kyligence.kap.rest.request.resourecegroup.ResourceGroupRequest;

@Order(100)
@Component
public class ResourceGroupDisabledValidator implements IResourceGroupRequestValidator {
    @Override
    public void validate(ResourceGroupRequest request) {
        if (request.isResourceGroupEnabled()) {
            return;
        }
        ResourceGroupManager manager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
        if (!request.isResourceGroupEnabled() && !manager.isResourceGroupEnabled()) {
            return;
        }
        if (CollectionUtils.isEmpty(request.getResourceGroupEntities())
                && CollectionUtils.isEmpty(request.getKylinInstances())
                && CollectionUtils.isEmpty(request.getResourceGroupMappingInfoList())) {
            return;
        }
        throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getResourceGroupDisabledWithInvliadParam());
    }
}
