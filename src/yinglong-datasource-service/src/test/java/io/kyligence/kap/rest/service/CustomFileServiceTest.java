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

import static io.kyligence.kap.metadata.jar.JarTypeEnum.STREAMING_CUSTOM_PARSER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_JAR_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_NOT_JAR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_UPLOAD_PARSER_LIMIT;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.jar.JarInfo;
import io.kyligence.kap.metadata.jar.JarInfoManager;
import io.kyligence.kap.metadata.streaming.DataParserInfo;
import io.kyligence.kap.metadata.streaming.DataParserManager;

public class CustomFileServiceTest extends NLocalFileMetadataTestCase {

    @Mock
    private final CustomFileService customFileService = Mockito.spy(CustomFileService.class);
    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    private static final String PROJECT = "streaming_test";
    private static final String JAR_NAME = "custom_parser.jar";
    private static final String JAR_TYPE = "STREAMING_CUSTOM_PARSER";
    private static String JAR_ABS_PATH;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(customFileService, "aclEvaluate", aclEvaluate);
        initJar();
    }

    public void initJar() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Path metaPath = new Path(kylinConfig.getMetadataUrl().toString());
        Path jarPath = new Path(String.format("%s/%s/%s", metaPath.getParent().toString(), "jars", JAR_NAME));
        JAR_ABS_PATH = new File(jarPath.toString()).toString();
    }

    @Test
    public void testCheckJarLegal() throws IOException {
        MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));
        customFileService.checkJarLegal(jarFile, PROJECT, JAR_TYPE);
        Assert.assertThrows(CUSTOM_PARSER_NOT_JAR.getMsg(JAR_NAME + ".txt"), KylinException.class,
                () -> customFileService.checkJarLegal(JAR_NAME + ".txt", PROJECT, JAR_TYPE));
    }

    @Test
    public void testCheckJarLegalExists() {
        JarInfoManager.getInstance(getTestConfig(), PROJECT)
                .createJarInfo(new JarInfo(PROJECT, JAR_NAME, "", STREAMING_CUSTOM_PARSER));
        Assert.assertThrows(CUSTOM_PARSER_JAR_EXISTS.getMsg(JAR_NAME), KylinException.class,
                () -> customFileService.checkJarLegal(JAR_NAME, PROJECT, JAR_TYPE));
    }

    @Test
    public void testUploadCustomJar() throws IOException {
        MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));
        String jar = customFileService.uploadCustomJar(jarFile, PROJECT, JAR_TYPE);
        Assert.assertNotNull(jar);
    }

    @Test
    public void testLoadParserJar() throws IOException {
        MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));
        Set<String> classList = customFileService.uploadJar(jarFile, PROJECT, JAR_TYPE);
        Assert.assertFalse(classList.isEmpty());
        customFileService.removeJar(PROJECT, JAR_NAME, JAR_TYPE);

        //Jar Type Error
        Assert.assertThrows(KylinException.class, () -> customFileService.uploadJar(jarFile, PROJECT, JAR_TYPE + "1"));
    }

    @Test
    public void testLoadParserJarOverLimit() throws IOException {
        MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));
        DataParserManager manager = DataParserManager.getInstance(getTestConfig(), PROJECT);
        manager.initDefault(PROJECT);
        for (int i = 0; i < 50; i++) {
            DataParserInfo parserInfo = new DataParserInfo(PROJECT, i + "", i + "");
            manager.createDataParserInfo(parserInfo);
        }
        Assert.assertTrue(manager.listDataParserInfo().size() - 1 <= 50);
        Assert.assertThrows(CUSTOM_PARSER_UPLOAD_PARSER_LIMIT.getMsg(getTestConfig().getCustomParserLimit()),
                KylinException.class, () -> customFileService.uploadJar(jarFile, PROJECT, JAR_TYPE));
    }

}
