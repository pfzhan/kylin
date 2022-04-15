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

import static org.junit.Assert.fail;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.DiagProgressRequest;
import io.kyligence.kap.rest.response.DiagStatusResponse;
import io.kyligence.kap.tool.constant.DiagTypeEnum;
import io.kyligence.kap.tool.constant.StageEnum;
import lombok.val;

public class SystemServiceTest extends NLocalFileMetadataTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @InjectMocks
    private SystemService systemService = Mockito.spy(new SystemService());

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

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
        diagInfo.setStage(StageEnum.DONE.toString());
        exportPathMap.put("test2", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", exportPathMap);
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        val result = systemService.getDiagPackagePath("test2");
        Assert.assertTrue(result.endsWith("diag.zip"));
    }

    @Test
    public void testGetExtractorStatus() throws Exception {
        Cache<String, SystemService.DiagInfo> extractorMap = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS).build();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo();
        extractorMap.put("test1", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", extractorMap);
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
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
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        systemService.stopDiagTask(uuid);

    }

    @Test
    public void testDumpLocalQueryDiagPackage() {
        overwriteSystemProp("kylin.security.allow-non-admin-generate-query-diag-package", "false");
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        systemService.dumpLocalQueryDiagPackage(null, null);
    }

    @Test
    public void testGetQueryDiagPackagePath() throws Exception {
        Cache<String, SystemService.DiagInfo> exportPathMap = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS).build();
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        File uuid = new File(mainDir, "uuid");
        File date = new File(uuid, "date");
        date.mkdirs();
        File zipFile = new File(date, "diag.zip");
        zipFile.createNewFile();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo(mainDir, null, DiagTypeEnum.QUERY);
        diagInfo.setExportFile(uuid);
        diagInfo.setStage(StageEnum.DONE.toString());
        exportPathMap.put("test2", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", exportPathMap);
        val result = systemService.getDiagPackagePath("test2");
        Assert.assertTrue(result.endsWith("diag.zip"));
}

    @Test
    public void testGetQueryExtractorStatus() {
        overwriteSystemProp("kylin.security.allow-non-admin-generate-query-diag-package", "false");
        Cache<String, SystemService.DiagInfo> extractorMap = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS).build();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo(null, null, DiagTypeEnum.QUERY);
        extractorMap.put("test1", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", extractorMap);
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        val result = systemService.getExtractorStatus("test1");
        Assert.assertEquals("PREPARE", ((DiagStatusResponse) result.getData()).getStage());
    }

    @Test
    public void testStopQueryDiagTask() throws Exception {
        String uuid = "testQuery";
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future task = executorService.submit(() -> {
        });
        task.get();
        Cache<String, SystemService.DiagInfo> futureMap = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS)
                .build();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo(null, task, DiagTypeEnum.FULL);
        diagInfo.setTask(task);
        futureMap.put(uuid, diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", futureMap);
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        systemService.stopDiagTask(uuid);
    }

    @Test
    public void testDumpLocalDiagPackage() {
        systemService.dumpLocalDiagPackage(null, null, "dd5a6451-0743-4b32-b84d-2ddc80524276", null, "test");
        systemService.dumpLocalDiagPackage(null, null, null, "5bc63cbe-a2fe-fa4e-3142-1bb4ebab8f98", "test");
    }

    @Test
    public void testUpdateDiagProgress() {
        Cache<String, SystemService.DiagInfo> extractorMap = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS).build();
        SystemService.DiagInfo diagInfo = new SystemService.DiagInfo();
        extractorMap.put("testUpdate", diagInfo);
        ReflectionTestUtils.setField(systemService, "diagMap", extractorMap);

        DiagProgressRequest diagProgressRequest = new DiagProgressRequest();
        diagProgressRequest.setDiagId("testUpdate");
        diagProgressRequest.setStage(StageEnum.DONE.toString());
        diagProgressRequest.setProgress(100);

        systemService.updateDiagProgress(diagProgressRequest);
        ReflectionTestUtils.setField(systemService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);

        val result = systemService.getExtractorStatus("testUpdate");
        Assert.assertEquals(StageEnum.DONE.toString(), result.getData().getStage());
        Assert.assertEquals(new Float(100), result.getData().getProgress());
    }

    @Test
    public void testRecoverMetadata() {
        try {
            systemService.reloadMetadata();
        } catch (Exception e) {
            fail("reload should be successful but not");
        }
    }

}
