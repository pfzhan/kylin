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

import static org.apache.kylin.common.exception.CommonErrorCode.UNKNOWN_ERROR_CODE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_CONFLICT_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.SEGMENT_EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_AUTH_INFO_NOTFOUND;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.service.ProjectService;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.controller.fake.HandleErrorController;

@RunWith(MockitoJUnitRunner.class)
public class BaseControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private final BaseController baseController = Mockito.spy(new BaseController());

    private final HandleErrorController handleErrorController = Mockito.spy(new HandleErrorController());

    private ProjectService projectService;

    @Before
    public void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(handleErrorController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        Mockito.when(handleErrorController.request()).thenThrow(new RuntimeException(), new ForbiddenException(),
                new NotFoundException(StringUtils.EMPTY), new AccessDeniedException(StringUtils.EMPTY),
                new UnauthorizedException(USER_AUTH_INFO_NOTFOUND), new KylinException(UNKNOWN_ERROR_CODE, StringUtils.EMPTY));
        createTestMetadata();

        projectService = Mockito.mock(ProjectService.class);
        ReflectionTestUtils.setField(baseController, "projectService", projectService);
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testHandleErrors() throws Exception {
        // assert handleError
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());

        // assert handleForbidden
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isForbidden());

        // assert handleNotFound
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isNotFound());

        // assert handleAccessDenied
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());

        // assert handleUnauthorized
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isUnauthorized());

        // assert handleErrorCode
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError());
    }

    @Test
    public void testCheckProjectException() {
        thrown.expect(KylinException.class);
        baseController.checkProjectName("");
    }

    @Test
    public void testCheckProjectPass() {
        baseController.checkProjectName("default");
        assert true;
    }

    @Test
    public void testCheckRequiredArgPass() {
        baseController.checkRequiredArg("model", "modelId");
        assert true;
    }

    @Test
    public void testCheckRequiredArgException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("'model' is required");
        baseController.checkRequiredArg("model", "");
    }

    @Test
    public void testCheckStartAndEndException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_RANGE_NOT_CONSISTENT());
        baseController.validateDataRange("10", "");
    }

    @Test
    public void testTimeRangeEndGreaterThanStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("The end time must be greater than the start time");
        baseController.validateDataRange("10", "1");
    }

    @Test
    public void testTimeRangeEndEqualToStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("The end time must be greater than the start time");
        baseController.validateDataRange("1", "1");
    }

    @Test
    public void testTimeRangeInvalidStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_RANGE_LESS_THAN_ZERO());
        baseController.validateDataRange("-1", "1");
    }

    @Test
    public void testTimeRangeInvalidEnd() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_RANGE_LESS_THAN_ZERO());
        baseController.validateDataRange("2", "-1");
    }

    @Test
    public void testTimeRangeInvalidFormat() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_RANGE_NOT_FORMAT());
        baseController.validateDataRange("start", "end");
    }

    @Test
    public void testTimeRangeValid() {
        baseController.validateDataRange("0", "86400000", "yyyy-MM-dd");
        baseController.validateDataRange("1000000000000", "2200000000000", "yyyy-MM-dd");
        baseController.validateDataRange("0", "86400000", PartitionDesc.TimestampType.MILLISECOND.name);
        baseController.validateDataRange("1000000000000", "2200000000000", PartitionDesc.TimestampType.SECOND.name);
    }

    @Test
    public void testTimeRangeEndEqualToStartWithDateFormat() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                Locale.getDefault(Locale.Category.FORMAT));
        String start = null;
        String end = null;
        try {
            start = Long.toString(format.parse("2012-01-01 00:00:00").getTime());
            end = Long.toString(format.parse("2012-01-01 06:00:00").getTime());
        } catch (Exception e) {
        }
        thrown.expect(KylinException.class);
        thrown.expectMessage("The end time must be greater than the start time");
        baseController.validateDataRange(start, end, "yyyy-MM-dd");
    }

    @Test
    public void testFormatStatus() {
        List<String> status = Lists.newArrayList("OFFLINE", null, "broken");
        Assert.assertEquals(baseController.formatStatus(status, ModelStatusToDisplayEnum.class),
                Lists.newArrayList("OFFLINE", "BROKEN"));

        thrown.expect(KylinException.class);
        thrown.expectMessage("is not a valid value");
        status = Lists.newArrayList("OFF", null, "broken");
        baseController.formatStatus(status, ModelStatusToDisplayEnum.class);
    }

    @Test
    public void testCheckParamLength() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Message.getInstance().getParamTooLarge(), "tag", 1000));
        List param = new ArrayList();
        param.add(1);
        param.add(6);
        param.add(String.join("", Collections.nCopies(1000, "l")));
        baseController.checkParamLength("tag", param, 1000);
    }

    @Test
    public void testGetProject() {
        Mockito.when(projectService.getReadableProjects(Mockito.anyString(), Mockito.anyBoolean()))
                .thenReturn(Collections.emptyList());
        try {
            baseController.getProject("SOME_PROJECT");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(PROJECT_NOT_EXIST.getCodeMsg("SOME_PROJECT"), e.getLocalizedMessage());
        }
    }

    @Test
    public void testCheckProjectName() {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = Mockito.mockStatic(KylinConfig.class);
             MockedStatic<NProjectManager> nProjectManagerMockedStatic = Mockito.mockStatic(NProjectManager.class)) {
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(Mockito.mock(KylinConfig.class));
            NProjectManager projectManager = Mockito.mock(NProjectManager.class);
            nProjectManagerMockedStatic.when(() -> NProjectManager.getInstance(Mockito.any())).thenReturn(projectManager);
            Mockito.when(projectManager.getProject(Mockito.anyString())).thenReturn(null);

            try {
                baseController.checkProjectName("SOME_PROJECT");
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof KylinException);
                Assert.assertEquals(PROJECT_NOT_EXIST.getCodeMsg("SOME_PROJECT"), e.getLocalizedMessage());
            }
        }
    }

    @Test
    public void testCheckSegmentParams() {
        try {
            baseController.checkSegmentParams(new String[] {"id1"}, new String[] {"name1"});
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(SEGMENT_CONFLICT_PARAMETER.getCodeMsg(), e.getLocalizedMessage());
        }

        try {
            baseController.checkSegmentParams(null, null);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(SEGMENT_EMPTY_PARAMETER.getCodeMsg(), e.getLocalizedMessage());
        }
    }

}
