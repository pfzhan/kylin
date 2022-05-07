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

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
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
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.service.CustomFileService;

public class CustomFileControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private CustomFileService customFileService = Mockito.spy(CustomFileService.class);

    @InjectMocks
    private CustomFileController customFileController = Mockito.spy(new CustomFileController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    private static final String PROJECT = "streaming_test";
    private static final String JAR_NAME = "custom_parser.jar";
    private static final String JAR_TYPE = "STREAMING_CUSTOM_PARSER";
    private static String JAR_ABS_PATH;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(customFileController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
        ReflectionTestUtils.setField(customFileController, "customFileService", customFileService);
    }

    @Before
    public void setupResource() {
        System.setProperty("HADOOP_USER_NAME", "root");
        createTestMetadata();
        initJar();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    private void initJar() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        // ../examples/test_data/21767/metadata
        Path metaPath = new Path(kylinConfig.getMetadataUrl().toString());
        Path jarPath = new Path(String.format("%s/%s/%s", metaPath.getParent().toString(), "jars", JAR_NAME));
        JAR_ABS_PATH = new File(jarPath.toString()).toString();
    }

    @Test
    public void testUploadNormal() throws Exception {
        MockMultipartFile jarFile = new MockMultipartFile("file", JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));

        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/custom/jar").file(jarFile)
                .contentType(MediaType.MULTIPART_FORM_DATA).param("project", PROJECT).param("jar_type", JAR_TYPE)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(customFileController).upload(Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testUploadEmpty() throws Exception {
        MockMultipartFile jarFile2 = new MockMultipartFile("file", "", "", (byte[]) null);
        mockMvc.perform(MockMvcRequestBuilders.fileUpload("/api/custom/jar").file(jarFile2)
                .contentType(MediaType.MULTIPART_FORM_DATA).param("project", PROJECT).param("jar_type", JAR_TYPE)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(customFileController).upload(Mockito.any(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testRemoveJara() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders.delete("/api/custom/jar").contentType(MediaType.APPLICATION_JSON)
                .param("project", PROJECT).param("jar_name", JAR_NAME).param("jar_type", JAR_TYPE)
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(customFileController).removeJar(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    }

}
