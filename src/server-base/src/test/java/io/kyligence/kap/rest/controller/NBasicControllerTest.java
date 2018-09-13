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

import io.kyligence.kap.rest.controller.fixture.FixtureController;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.exception.UnauthorizedException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class NBasicControllerTest {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @InjectMocks
    private NBasicController nBasicController = Mockito.spy(new NBasicController());

    private final FixtureController fixtureController = Mockito.spy(new FixtureController());

    @Before
    public void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(fixtureController)
                .defaultRequest(MockMvcRequestBuilders.get("/").servletPath("/api")).build();

        Mockito.when(fixtureController.request()).thenThrow(new RuntimeException(), new ForbiddenException(),
                new NotFoundException(StringUtils.EMPTY), new BadRequestException(StringUtils.EMPTY),
                new UnauthorizedException());
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

        // assert handleBadRequest
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());

        // assert handleUnauthorized
        mockMvc.perform(MockMvcRequestBuilders.get("/api/handleErrors"))
                .andExpect(MockMvcResultMatchers.status().isUnauthorized());
    }

    @Test
    public void testCheckProjectException() {
        thrown.expect(BadRequestException.class);
        nBasicController.checkProjectName("");
    }

    @Test
    public void testCheckProjectPass() {
        nBasicController.checkProjectName("default");
        assert true;
    }

    @Test
    public void testCheckRequiredArgPass() {
        nBasicController.checkRequiredArg("model", "modelName");
        assert true;
    }

    @Test
    public void testCheckRequiredArgException() {
        thrown.expect(BadRequestException.class);
        thrown.expectMessage("model is required");
        nBasicController.checkRequiredArg("model", "");
    }

}
