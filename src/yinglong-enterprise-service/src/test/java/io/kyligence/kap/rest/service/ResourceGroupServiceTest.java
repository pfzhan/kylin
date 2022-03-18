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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.Map;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import io.kyligence.kap.metadata.resourcegroup.KylinInstance;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroup;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupEntity;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupMappingInfo;
import io.kyligence.kap.rest.request.resourecegroup.ResourceGroupRequest;
import io.kyligence.kap.rest.service.resourcegroup.ResourceGroupService;
import lombok.val;

public class ResourceGroupServiceTest extends NLocalFileMetadataTestCase {
    @InjectMocks
    private final ResourceGroupService resourceGroupService = Mockito.spy(ResourceGroupService.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(resourceGroupService, "aclEvaluate", aclEvaluate);
    }

    @After
    public void tearDown() {
        getTestConfig().setProperty("kylin.metadata.semi-automatic-mode", "false");
        cleanupTestMetadata();
    }

    @Test
    public void testUpdateResourceGroup() throws IOException {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        Map<String, String> map2 = Maps.newHashMap();
        map2.put("id", "124");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        val entity2 = JsonUtil.readValue(new Gson().toJson(map2), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1, entity2));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        map.clear();
        map.put("project", "default");
        map.put("resource_group_id", "123");
        map.put("request_type", "BUILD");

        map2.clear();
        map2.put("project", "default");
        map2.put("resource_group_id", "124");
        map2.put("request_type", "QUERY");

        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        val mapping2 = JsonUtil.readValue(new Gson().toJson(map2), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping, mapping2));
        resourceGroupService.updateResourceGroup(request);
        Mockito.verify(resourceGroupService, Mockito.times(1)).updateResourceGroup(request);
    }

    @Test
    public void testGetResourceGroup() {
        ResourceGroup resourceGroup = resourceGroupService.getResourceGroup();
        Assert.assertFalse(resourceGroup.isResourceGroupEnabled());
        Mockito.verify(resourceGroupService, Mockito.times(1)).getResourceGroup();
    }
}
