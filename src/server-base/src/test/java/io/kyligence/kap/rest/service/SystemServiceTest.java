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

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.response.DiagStatusResponse;
import io.kyligence.kap.tool.DiagClientTool;
import lombok.val;

public class SystemServiceTest extends NLocalFileMetadataTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @InjectMocks
    private SystemService systemService = Mockito.spy(new SystemService());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetDiagPackagePath() throws Exception {
        Cache<String, SystemService.DiagInfo> exportPathMap = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS).build();
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        File uuid = new File(mainDir, "uuid");
        File date = new File(uuid, "date");
        date.mkdirs();
        File zipFile = new File(date, "diag.zip");
        zipFile.createNewFile();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo();
        diagInfo.setExportFile(uuid);
        exportPathMap.put("test2", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", exportPathMap);
        val result = systemService.getDiagPackagePath("test2");
        Assert.assertTrue(result.endsWith("diag.zip"));
    }

    @Test
    public void testGetExtractorStatus() throws Exception {
        Cache<String, SystemService.DiagInfo> extractorMap = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS).build();
        DiagClientTool diagClientTool = new DiagClientTool();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo();
        diagInfo.setExtractor(diagClientTool);
        extractorMap.put("test1", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", extractorMap);
        val result = systemService.getExtractorStatus("test1");
        Assert.assertEquals("PREPARE", ((DiagStatusResponse) result.getData()).getStage());
    }

    @Test
    public void testStopDiagTask() throws Exception {
        String uuid = "test3";
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future task = executorService.submit(() -> {
        });
        task.get();
        Cache<String, SystemService.DiagInfo> futureMap = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS)
                .build();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo();
        diagInfo.setTask(task);
        futureMap.put(uuid, diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", futureMap);
        val result = systemService.stopDiagTask(uuid);
        Assert.assertFalse(result);
    }

}
