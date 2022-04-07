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

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.resourcegroup.KylinInstance;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroup;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupEntity;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupManager;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupMappingInfo;
import io.kyligence.kap.rest.handler.resourcegroup.IResourceGroupRequestValidator;
import io.kyligence.kap.rest.handler.resourcegroup.ResourceGroupDisabledValidator;
import io.kyligence.kap.rest.handler.resourcegroup.ResourceGroupEnabledValidator;
import io.kyligence.kap.rest.handler.resourcegroup.ResourceGroupEntityValidator;
import io.kyligence.kap.rest.handler.resourcegroup.ResourceGroupFieldValidator;
import io.kyligence.kap.rest.handler.resourcegroup.ResourceGroupKylinInstanceValidator;
import io.kyligence.kap.rest.handler.resourcegroup.ResourceGroupMappingInfoValidator;
import io.kyligence.kap.rest.request.resourecegroup.ResourceGroupRequest;
import io.kyligence.kap.rest.service.resourcegroup.ResourceGroupService;
import lombok.val;

public class ResourceGroupControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;

    @Spy
    private List<IResourceGroupRequestValidator> requestFilterList = Lists.newArrayList();

    @Mock
    private ResourceGroupService resourceGroupService;

    @InjectMocks
    private ResourceGroupController resourceGroupController = Mockito.spy(new ResourceGroupController());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(resourceGroupController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
        requestFilterList.add(new ResourceGroupFieldValidator());
        requestFilterList.add(new ResourceGroupDisabledValidator());
        requestFilterList.add(new ResourceGroupEnabledValidator());
        requestFilterList.add(new ResourceGroupEntityValidator());
        requestFilterList.add(new ResourceGroupKylinInstanceValidator());
        requestFilterList.add(new ResourceGroupMappingInfoValidator());
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    //=============  ResourceGroupFieldValidator  ===============

    @Test
    public void testResourceGroupFieldFilterException1() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getRESOURCE_GROUP_FIELD_IS_NULL());
        resourceGroupController.updateResourceGroup(request);

    }

    @Test
    public void testResourceGroupFieldFilterException2() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setKylinInstances(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getRESOURCE_GROUP_FIELD_IS_NULL());
        resourceGroupController.updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupFieldFilterException3() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getRESOURCE_GROUP_FIELD_IS_NULL());
        resourceGroupController.updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupFieldFilterException4() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getRESOURCE_GROUP_FIELD_IS_NULL());
        resourceGroupController.updateResourceGroup(request);
    }

    //=============  ResourceGroupDisabledValidator  ===============

    @Test
    public void testResourceGroupDisabledFilter1() throws Exception {
        setResourceGroupEnabled();

        Mockito.doReturn(Mockito.mock(ResourceGroup.class)).when(resourceGroupService).getResourceGroup();

        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(false);
        request.setResourceGroupMappingInfoList(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());
        request.setKylinInstances(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(resourceGroupController).updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupDisabledFilter2() throws Exception {
        Mockito.doReturn(Mockito.mock(ResourceGroup.class)).when(resourceGroupService).getResourceGroup();

        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(false);
        request.setResourceGroupMappingInfoList(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());
        request.setKylinInstances(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(resourceGroupController).updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupDisabledFilterException1() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(false);
        request.setResourceGroupMappingInfoList(Lists.newArrayList());
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());
        Mockito.doReturn(Mockito.mock(ResourceGroup.class)).when(resourceGroupService).getResourceGroup();
        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(resourceGroupController).updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupDisabledFilterException2() throws Exception {
        setResourceGroupEnabled();

        ResourceGroupRequest request = new ResourceGroupRequest();
        List<KylinInstance> kylinInstances = Lists.newArrayList(new KylinInstance(), new KylinInstance());
        request.setKylinInstances(kylinInstances);
        request.setResourceGroupMappingInfoList(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getRESOURCE_GROUP_DISABLED_WITH_INVLIAD_PARAM());
        resourceGroupController.updateResourceGroup(request);
    }

    //=============  ResourceGroupEnabledValidator  ===============

    @Test
    public void testResourceGroupEnabledFilterException1() throws Exception {
        setResourceGroupEnabled();

        Mockito.doReturn(Mockito.mock(ResourceGroup.class)).when(resourceGroupService).getResourceGroup();
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupMappingInfoList(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getRESOURCE_CAN_NOT_BE_EMPTY());
        resourceGroupController.updateResourceGroup(request);
    }

    //=============  ResourceGroupEntityValidator  ===============

    @Test
    public void testResourceGroupEntityFilterException1() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);
        request.setResourceGroupEntities(Lists.newArrayList(new ResourceGroupEntity(), new ResourceGroupEntity()));
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getEMPTY_RESOURCE_GROUP_ID());
        resourceGroupController.updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupEntityFilterException2() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);
        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");

        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        val entity2 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);

        request.setResourceGroupEntities(Lists.newArrayList(entity1, entity2));
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getDUPLICATED_RESOURCE_GROUP_ID("123"));
        resourceGroupController.updateResourceGroup(request);
    }

    //=============  ResourceGroupEntityValidator  ===============

    @Test
    public void testResourceGroupKylinInstanceFilterException1() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);

        request.setResourceGroupEntities(Lists.newArrayList(entity1));
        request.setKylinInstances(Lists.newArrayList(new KylinInstance()));
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getEMPTY_KYLIN_INSTANCE_IDENTITY());
        resourceGroupController.updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupKylinInstanceFilterException2() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getEMPTY_KYLIN_INSTANCE_RESOURCE_GROUP_ID());
        resourceGroupController.updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupKylinInstanceFilterException3() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "1");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getRESOURCE_GROUP_ID_NOT_EXIST_IN_KYLIN_INSTANCE("1"));
        resourceGroupController.updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupKylinInstanceFilterException4() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");

        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance, instance));
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getDUPLICATED_KYLIN_INSTANCE());
        resourceGroupController.updateResourceGroup(request);
    }

    //=============  ResourceGroupMappingInfoValidator  ===============

    @Test
    public void testResourceGroupMappingInfoFilterException1() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        request.setResourceGroupMappingInfoList(Lists.newArrayList(new ResourceGroupMappingInfo()));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getEMPTY_PROJECT_IN_MAPPING_INFO());
        resourceGroupController.updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupMappingInfoFilterException2() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        map.clear();
        map.put("project", "213");
        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage("Can't find project \"213\". Please check and try again.");
        resourceGroupController.updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupMappingInfoFilterException3() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        map.clear();
        map.put("project", "default");
        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getEMPTY_RESOURCE_GROUP_ID_IN_MAPPING_INFO());
        resourceGroupController.updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupMappingInfoFilterException4() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        map.clear();
        map.put("project", "default");
        map.put("resource_group_id", "1");
        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getRESOURCE_GROUP_ID_NOT_EXIST_IN_MAPPING_INFO("1"));
        resourceGroupController.updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupMappingInfoFilterException5() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        Map<String, String> map2 = Maps.newHashMap();
        map2.put("id", "124");
        Map<String, String> map3 = Maps.newHashMap();
        map3.put("id", "125");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        val entity2 = JsonUtil.readValue(new Gson().toJson(map2), ResourceGroupEntity.class);
        val entity3 = JsonUtil.readValue(new Gson().toJson(map3), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1, entity2, entity3));

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

        map3.clear();
        map3.put("project", "default");
        map3.put("resource_group_id", "125");
        map3.put("request_type", "QUERY");

        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        val mapping2 = JsonUtil.readValue(new Gson().toJson(map2), ResourceGroupMappingInfo.class);
        val mapping3 = JsonUtil.readValue(new Gson().toJson(map3), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping, mapping2, mapping3));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT,
                Message.getInstance().getPROJECT_BINDING_RESOURCE_GROUP_INVALID(), map.get("project")));
        resourceGroupController.updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupMappingInfoFilterException6() throws Exception {
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
        map2.put("request_type", "BUILD");

        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        val mapping2 = JsonUtil.readValue(new Gson().toJson(map2), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping, mapping2));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Locale.ROOT,
                Message.getInstance().getPROJECT_BINDING_RESOURCE_GROUP_INVALID(), map.get("project")));
        resourceGroupController.updateResourceGroup(request);
    }

    //=============  pass case  ===============

    @Test
    public void testPassCase() throws Exception {
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

        Mockito.doReturn(Mockito.mock(ResourceGroup.class)).when(resourceGroupService).getResourceGroup();

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        ResourceGroup resourceGroup = new ResourceGroup();
        resourceGroup.setResourceGroupEnabled(true);
        resourceGroup.setResourceGroupEntities(Lists.newArrayList(entity1));
        Mockito.doReturn(resourceGroup).when(resourceGroupService).getResourceGroup();

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        resourceGroup.setResourceGroupEnabled(true);
        resourceGroup.setResourceGroupEntities(Lists.newArrayList(entity1, entity2));
        resourceGroup.setKylinInstances(Lists.newArrayList());
        Mockito.doReturn(resourceGroup).when(resourceGroupService).getResourceGroup();

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        resourceGroup.setResourceGroupEnabled(true);
        resourceGroup.setResourceGroupEntities(Lists.newArrayList(entity1, entity2));
        resourceGroup.setKylinInstances(Lists.newArrayList(instance));
        resourceGroup.setResourceGroupMappingInfoList(Lists.newArrayList(mapping));
        Mockito.doReturn(resourceGroup).when(resourceGroupService).getResourceGroup();

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(resourceGroupController, Mockito.times(4)).updateResourceGroup(request);
    }

    //=============  methods  ===============

    private void setResourceGroupEnabled() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        manager.getResourceGroup();
        manager.updateResourceGroup(copyForWrite -> {
            copyForWrite.setResourceGroupEnabled(true);
        });
    }

}
