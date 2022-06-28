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
package io.kyligence.kap.rest;

import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.exception.KylinException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import reactor.core.publisher.Mono;

public class ProjectBasedLoadBalancerTest extends NLocalFileMetadataTestCase {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testChooseWithEmptyOwner() throws IOException {
        createHttpServletRequestMock("default2");
        exceptionRule.expect(KylinException.class);
        exceptionRule.expectMessage("System is trying to recover service. Please try again later.");
        Mono.from(new ProjectBasedLoadBalancer().choose()).block();
    }

    private void createHttpServletRequestMock(String project) throws IOException {
        HttpServletRequest request = Mockito.spy(HttpServletRequest.class);
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
        when(request.getParameter("project")).thenReturn(project);

        val bodyJson = "{\"project\": \"" + project + "\"}";
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(
                bodyJson.getBytes(StandardCharsets.UTF_8));

        when(request.getInputStream()).thenReturn(new ServletInputStream() {
            @Override
            public boolean isFinished() {
                return false;
            }

            @Override
            public boolean isReady() {
                return false;
            }

            @Override
            public void setReadListener(ReadListener listener) {

            }

            private boolean isFinished;

            @Override
            public int read() {
                int b = byteArrayInputStream.read();
                isFinished = b == -1;
                return b;
            }
        });
    }

    private void createTestProjectAndEpoch(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());
        projectManager.createProject(project, "abcd", "", null);
        EpochManager.getInstance().updateEpochWithNotifier(project, false);

    }

    private void testChooseInternal() {

        String instance = AddressUtil.getLocalInstance();
        String[] split = instance.split(":");

        ServiceInstance server = Mono.from(new ProjectBasedLoadBalancer().choose()).block().getServer();
        Assert.assertEquals(split[0], server.getHost());
        Assert.assertEquals(Integer.parseInt(split[1]), server.getPort());
    }

    /**
     * test different project params that in request body
     * choose the given project' epoch
     * @param requestProject
     * @param project
     * @throws IOException
     */
    private void testRequestAndChooseOwner(String requestProject, String project) throws IOException {
        createHttpServletRequestMock(requestProject);
        createTestProjectAndEpoch(project);
        testChooseInternal();
    }

    @Test
    public void testChoose() throws IOException {

        {
            testRequestAndChooseOwner("TEst_ProJECT2", "test_project2");
        }

        {
            testRequestAndChooseOwner("TEST_PROJECT", "test_project");
        }
    }
}