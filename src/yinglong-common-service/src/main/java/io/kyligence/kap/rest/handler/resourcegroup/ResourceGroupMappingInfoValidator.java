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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.resourcegroup.RequestTypeEnum;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupEntity;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupMappingInfo;
import io.kyligence.kap.rest.request.resourecegroup.ResourceGroupRequest;

@Order(500)
@Component
public class ResourceGroupMappingInfoValidator implements IResourceGroupRequestValidator {
    @Override
    public void validate(ResourceGroupRequest request) {
        if (!request.isResourceGroupEnabled()) {
            return;
        }
        // check project exist and not empty
        List<String> resourceGroups = request.getResourceGroupEntities().stream().map(ResourceGroupEntity::getId)
                .collect(Collectors.toList());
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<ResourceGroupMappingInfo> mappingInfo = request.getResourceGroupMappingInfoList();
        for (ResourceGroupMappingInfo info : mappingInfo) {
            if (StringUtils.isBlank(info.getProject())) {
                throw new KylinException(INVALID_PARAMETER, MsgPicker.getMsg().getEMPTY_PROJECT_IN_MAPPING_INFO());
            }
            ProjectInstance prjInstance = projectManager.getProject(info.getProject());
            if (prjInstance == null) {
                throw new KylinException(PROJECT_NOT_EXIST, info.getProject());
            }
            if (StringUtils.isBlank(info.getResourceGroupId())) {
                throw new KylinException(INVALID_PARAMETER,
                        MsgPicker.getMsg().getEMPTY_RESOURCE_GROUP_ID_IN_MAPPING_INFO());
            }
            if (!resourceGroups.contains(info.getResourceGroupId())) {
                throw new KylinException(INVALID_PARAMETER,
                        MsgPicker.getMsg().getRESOURCE_GROUP_ID_NOT_EXIST_IN_MAPPING_INFO(info.getResourceGroupId()));
            }
        }

        // check the relationship between project and resource group
        Map<String, List<ResourceGroupMappingInfo>> projectMappingInfo = mappingInfo.stream()
                .collect(Collectors.groupingBy(ResourceGroupMappingInfo::getProject));
        for (Map.Entry<String, List<ResourceGroupMappingInfo>> entry : projectMappingInfo.entrySet()) {
            String project = entry.getKey();
            List<ResourceGroupMappingInfo> projectMapping = entry.getValue();

            boolean bindInvalidTotalNum = projectMapping.size() > 2;
            boolean bindInvalidNumInOneType = projectMapping.stream()
                    .filter(info -> info.getRequestType() == RequestTypeEnum.BUILD).count() > 1
                    || projectMapping.stream().filter(info -> info.getRequestType() == RequestTypeEnum.QUERY)
                            .count() > 1;

            if (bindInvalidTotalNum || bindInvalidNumInOneType) {
                throw new KylinException(INVALID_PARAMETER, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getPROJECT_BINDING_RESOURCE_GROUP_INVALID(), project));
            }
        }
    }
}
