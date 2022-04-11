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
import static org.apache.kylin.common.exception.code.ErrorCodeServer.USER_AUTH_INFO_NOTFOUND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import io.kyligence.kap.rest.controller.fixture.FixtureController;

public class NBasicControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NBasicController nBasicController = Mockito.spy(new NBasicController());

    private final FixtureController fixtureController = Mockito.spy(new FixtureController());

    @Before
    public void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(fixtureController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();

        Mockito.when(fixtureController.request()).thenThrow(new RuntimeException(), new ForbiddenException(),
                new NotFoundException(StringUtils.EMPTY), new AccessDeniedException(StringUtils.EMPTY),
                new UnauthorizedException(USER_AUTH_INFO_NOTFOUND), new KylinException(UNKNOWN_ERROR_CODE, StringUtils.EMPTY));
        createTestMetadata();
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
    public void testGetProject_throwsException() {
        try {
            nBasicController.getProject(null);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(PROJECT_NOT_EXIST.getCodeMsg(null), e.getLocalizedMessage());
        }
    }

    @Test
    public void testCheckProjectException() {
        thrown.expect(KylinException.class);
        nBasicController.checkProjectName("");
    }

    @Test
    public void testCheckProjectPass() {
        nBasicController.checkProjectName("default");
        assert true;
    }

    @Test
    public void testCheckRequiredArgPass() {
        nBasicController.checkRequiredArg("model", "modelId");
        assert true;
    }

    @Test
    public void testCheckRequiredArgException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("'model' is required");
        nBasicController.checkRequiredArg("model", "");
    }

    @Test
    public void testCheckStartAndEndException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_RANGE_NOT_CONSISTENT());
        nBasicController.validateDataRange("10", "");
    }

    @Test
    public void testTimeRangeEndGreaterThanStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("The end time must be greater than the start time");
        nBasicController.validateDataRange("10", "1");
    }

    @Test
    public void testTimeRangeEndEqualToStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("The end time must be greater than the start time");
        nBasicController.validateDataRange("1", "1");
    }

    @Test
    public void testTimeRangeInvalidStart() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_RANGE_LESS_THAN_ZERO());
        nBasicController.validateDataRange("-1", "1");
    }

    @Test
    public void testTimeRangeInvalidEnd() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_RANGE_LESS_THAN_ZERO());
        nBasicController.validateDataRange("2", "-1");
    }

    @Test
    public void testTimeRangeInvalidFormat() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(Message.getInstance().getINVALID_RANGE_NOT_FORMAT());
        nBasicController.validateDataRange("start", "end");
    }

    @Test
    public void testTimeRangeValid() {
        nBasicController.validateDataRange("0", "86400000", "yyyy-MM-dd");
        nBasicController.validateDataRange("1000000000000", "2200000000000", "yyyy-MM-dd");
        nBasicController.validateDataRange("0", "86400000", PartitionDesc.TimestampType.MILLISECOND.name);
        nBasicController.validateDataRange("1000000000000", "2200000000000", PartitionDesc.TimestampType.SECOND.name);
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
        nBasicController.validateDataRange(start, end, "yyyy-MM-dd");
    }

    @Test
    public void testFormatStatus() {
        List<String> status = Lists.newArrayList("OFFLINE", null, "broken");
        assertEquals(nBasicController.formatStatus(status, ModelStatusToDisplayEnum.class),
                Lists.newArrayList("OFFLINE", "BROKEN"));

        thrown.expect(KylinException.class);
        thrown.expectMessage("is not a valid value");
        status = Lists.newArrayList("OFF", null, "broken");
        nBasicController.formatStatus(status, ModelStatusToDisplayEnum.class);
    }

    @Test
    public void testCheckParamLength() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(String.format(Message.getInstance().getParamTooLarge(), "tag", 1000));
        List param = new ArrayList();
        param.add(1);
        param.add(6);
        param.add(String.join("", Collections.nCopies(1000, "l")));
        nBasicController.checkParamLength("tag", param, 1000);
    }

    @Test
    public void testCheckStreamingEnabled() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getStreamingDisabled());
        getTestConfig().setProperty("kylin.streaming.enabled", "false");
        nBasicController.checkStreamingEnabled();
    }

    @Test
    public void testCheckSegmentParms_throwsException() {
        String[] ids = new String[] {"TEST_ID1"};
        String[] names = new String[] {"TEST_NAME1"};

        // test throwing SEGMENT_CONFLICT_PARAMETER
        try {
            nBasicController.checkSegmentParms(ids, names);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-010022214: Can't enter segment ID and name at the same time. Please re-enter.", e.toString());
        }

        // test throwing SEGMENT_EMPTY_PARAMETER
        try {
            nBasicController.checkSegmentParms(null, null);
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals("KE-010022215: Please enter segment ID or name.", e.toString());
        }
    }
}
