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

import io.kyligence.kap.clickhouse.MockSecondStorage;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.ArrayList;

public class ProjectServiceWithSecondStorageTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private final ProjectService projectService = Mockito.spy(ProjectService.class);

    @InjectMocks
    private final ProjectSmartServiceSupporter projectSmartService = Mockito.spy(ProjectSmartServiceSupporter.class);

    @InjectMocks
    private final ModelService modelService = Mockito.spy(ModelService.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AsyncTaskServiceSupporter asyncTaskService = Mockito.spy(AsyncTaskServiceSupporter.class);

    @Mock
    private final AccessService accessService = Mockito.spy(AccessService.class);

    @Mock
    private final UserService userService = Mockito.spy(UserService.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private NProjectManager projectManager;

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        overwriteSystemProp("kylin.cube.low-frequency-threshold", "5");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(projectService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(projectService, "accessService", accessService);
        ReflectionTestUtils.setField(projectService, "projectModelSupporter", modelService);
        ReflectionTestUtils.setField(projectService, "userService", userService);
        ReflectionTestUtils.setField(projectService, "projectSmartService", projectSmartService);

        ReflectionTestUtils.setField(modelService, "aclEvaluate", aclEvaluate);
        projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
    }

    @Test(expected = KylinException.class)
    public void testDropProjectFailed() throws IOException {
        val project = "default";
        MockSecondStorage.mock(project, new ArrayList<>(), this);
        projectService.dropProject(project);
    }

}
