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

package io.kyligence.kap.rest.config.initialize;

import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.metadata.model.FusionModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.rest.service.FusionModelService;
import io.kyligence.kap.rest.service.ModelService;
import io.kyligence.kap.rest.service.NUserGroupService;
import io.kyligence.kap.rest.service.SourceTestCase;

public class ModelUpdateListenerTest extends SourceTestCase {

    @InjectMocks
    private FusionModelService fusionModelService = Mockito.spy(new FusionModelService());

    @InjectMocks
    private ModelService modelService = Mockito.spy(new ModelService());

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    private final ModelUpdateListener modelUpdateListener = new ModelUpdateListener();

    @Before
    public void setup() {
        super.setup();
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        EventBusFactory.getInstance().register(modelUpdateListener, true);
        ReflectionTestUtils.setField(fusionModelService, "modelService", modelService);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(modelService, "accessService", accessService);
        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(modelService, "userGroupService", userGroupService);
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testRenameFusionModelName() {
        String modelId = "b05034a8-c037-416b-aa26-9e6b4a41ee40";
        String batchId = "334671fd-e383-4fc9-b5c2-94fce832f77a";
        String project = "streaming_test";
        String newModelName = "new_streaming";
        fusionModelService.renameDataModel(project, modelId, newModelName);
        Assert.assertEquals(newModelName,
                NDataModelManager.getInstance(getTestConfig(), project).getDataModelDesc(modelId).getAlias());
        Assert.assertEquals(FusionModel.getBatchName(newModelName, modelId),
                NDataModelManager.getInstance(getTestConfig(), project).getDataModelDesc(batchId).getAlias());
    }

    @Test
    public void testRenameStreamingModelName() {
        String modelId = "e78a89dd-847f-4574-8afa-8768b4228b73";
        String project = "streaming_test";
        String newModelName = "new_streaming2";
        fusionModelService.renameDataModel(project, modelId, newModelName);
        Assert.assertEquals(newModelName,
                NDataModelManager.getInstance(getTestConfig(), project).getDataModelDesc(modelId).getAlias());
    }
}
