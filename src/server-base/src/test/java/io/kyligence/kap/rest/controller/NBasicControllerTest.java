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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.kylin.common.exception.KylinException;
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

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.controller.fixture.FixtureController;

import java.text.SimpleDateFormat;

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
                new UnauthorizedException(), new KylinException(UNKNOWN_ERROR_CODE, StringUtils.EMPTY));
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
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
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
        thrown.expectMessage("model is required");
        nBasicController.checkRequiredArg("model", "");
    }

    @Test
    public void testCheckStartAndEndException() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Start and end must exist or not at the same time!");
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
        thrown.expectMessage("Start or end of range must be greater than 0!");
        nBasicController.validateDataRange("-1", "1");
    }

    @Test
    public void testTimeRangeInvalidEnd() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Start or end of range must be greater than 0!");
        nBasicController.validateDataRange("2", "-1");
    }

    @Test
    public void testTimeRangeInvalidFormat() {
        thrown.expect(KylinException.class);
        thrown.expectMessage("Invalid start or end time format. Only support timestamp type, unit ms");
        nBasicController.validateDataRange("start", "end");
    }

    @Test
    public void testTimeRangeValid() {
        nBasicController.validateDataRange("0", "86400000", "yyyy-MM-dd");
        nBasicController.validateDataRange("1000000000000", "2200000000000", "yyyy-MM-dd");
    }

    @Test
    public void testTimeRangeEndEqualToStartWithDateFormat() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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

}
