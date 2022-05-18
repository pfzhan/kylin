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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.resourcegroup.KylinInstance;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupEntity;
import io.kyligence.kap.rest.request.resourecegroup.ResourceGroupRequest;
import lombok.val;

@Order(400)
@Component
public class ResourceGroupKylinInstanceValidator implements IResourceGroupRequestValidator {
    @Override
    public void validate(ResourceGroupRequest request) {
        if (!request.isResourceGroupEnabled()) {
            return;
        }
        List<String> resourceGroups = request.getResourceGroupEntities().stream().map(ResourceGroupEntity::getId)
                .collect(Collectors.toList());
        List<KylinInstance> instances = request.getKylinInstances();
        for (KylinInstance instance : instances) {
            if (StringUtils.isBlank(instance.getInstance())) {
                throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getEmptyKylinInstanceIdentity());
            }
            if (StringUtils.isBlank(instance.getResourceGroupId())) {
                throw new KylinException(INVALID_PARAMETER,
                        MsgPicker.getMsg().getEmptyKylinInstanceResourceGroupId());
            }
            if (!resourceGroups.contains(instance.getResourceGroupId())) {
                throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg()
                        .getResourceGroupIdNotExistInKylinInstance(instance.getResourceGroupId()));
            }
        }
        val identities = instances.stream().map(KylinInstance::getInstance).collect(Collectors.toList());
        if (identities.size() != Sets.newHashSet(identities).size()) {
            throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getDuplicatedKylinInstance());
        }
    }
}
