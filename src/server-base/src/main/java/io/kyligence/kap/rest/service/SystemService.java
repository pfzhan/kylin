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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import io.kyligence.kap.rest.request.BackupRequest;
import io.kyligence.kap.rest.response.DiagStatusResponse;
import io.kyligence.kap.tool.AbstractInfoExtractorTool;
import io.kyligence.kap.tool.DiagClientTool;
import io.kyligence.kap.tool.JobDiagInfoTool;
import io.kyligence.kap.tool.MetadataTool;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Service("systemService")
@Slf4j
public class SystemService extends BasicService {

    private Cache<String, AbstractInfoExtractorTool> extractorMap = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.DAYS).build();
    private Cache<String, File> exportPathMap = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS).build();
    private Cache<String, DiagStatusResponse> exceptionMap = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.DAYS).build();
    private ExecutorService executorService = Executors.newFixedThreadPool(3);

    public void backup(BackupRequest backupRequest) throws Exception {
        String[] args = createBackupArgs(backupRequest);
        val metadataTool = new MetadataTool(getConfig());
        metadataTool.execute(args);
    }

    private String[] createBackupArgs(BackupRequest backupRequest) {
        List<String> args = Lists.newArrayList("-backup", "-dir", backupRequest.getBackupPath());
        if (backupRequest.isCompress()) {
            args.add("-compress");
        }
        if (StringUtils.isNotBlank(backupRequest.getProject())) {
            args.add("-project");
            args.add(backupRequest.getProject());
        }

        log.info("SystemService " + args);
        return args.toArray(new String[0]);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String dumpLocalDiagPackage(String startTime, String endTime, String jobId) throws IOException {
        File exportFile = KylinConfigBase.getDiagFileName();
        String uuid = exportFile.getName();
        FileUtils.deleteQuietly(exportFile);
        exportFile.mkdirs();
        exportPathMap.put(uuid, exportFile);

        AbstractInfoExtractorTool extractor;
        String[] arguments;
        if (StringUtils.isEmpty(jobId)) {//full
            if (startTime == null && endTime == null) {
                startTime = Long.toString(System.currentTimeMillis() - 259200000L);
                endTime = Long.toString(System.currentTimeMillis());
            }
            extractor = new DiagClientTool();
            arguments = new String[] { "-destDir", exportFile.getAbsolutePath(), "-startTime", startTime, "-endTime",
                    endTime };
        } else {//job
            extractor = new JobDiagInfoTool();
            arguments = new String[] { "-destDir", exportFile.getAbsolutePath(), "-job", jobId };
        }
        extractorMap.put(uuid, extractor);
        executorService.execute(() -> {
            try {
                exceptionMap.invalidate(uuid);
                extractor.execute(arguments);
            } catch (Exception ex) {
                handleDiagException(uuid, ex);
            }
        });
        return uuid;
    }

    private void handleDiagException(String uuid, @NotNull Exception ex) {
        log.warn("Diagnostic kit error", ex);
        Throwable cause = ex;
        while (cause != null && cause.getCause() != null) {
            cause = cause.getCause();
        }
        DiagStatusResponse response = new DiagStatusResponse();
        if (cause instanceof KylinTimeoutException) {
            response.setStatus("001");
        } else if (cause instanceof IOException || cause instanceof AccessDeniedException) {
            response.setStatus("002");
        } else {
            response.setStatus("999");
        }
        response.setError(cause == null ? ex.getMessage() : cause.getMessage());
        exceptionMap.put(uuid, response);
        FileUtils.deleteQuietly(exportPathMap.getIfPresent(uuid));
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String getDiagPackagePath(String uuid) throws Exception {
        DiagStatusResponse exception = exceptionMap.getIfPresent(uuid);
        if (exception != null) {
            throw new RuntimeException(exception.getError());
        }
        val extractor = extractorMap.getIfPresent(uuid);
        if (extractor != null && !extractor.getStage().equals("DONE")) {
            throw new RuntimeException("Diagnostic task is running now , can not download yet");
        }
        File exportFile = exportPathMap.getIfPresent(uuid);
        if (exportFile == null) {
            throw new RuntimeException("ID {" + uuid + "} is not exist");
        }
        String zipFilePath = findZipFile(exportFile);
        if (zipFilePath == null) {
            throw new RuntimeException(
                    String.format(MsgPicker.getMsg().getDIAG_PACKAGE_NOT_AVAILABLE(), exportFile.getAbsoluteFile()));
        }
        return zipFilePath;
    }

    private String findZipFile(File rootDir) {
        if (rootDir == null)
            return null;
        File[] files = rootDir.listFiles();
        if (files == null)
            return null;
        for (File subFile : files) {
            if (subFile.isDirectory()) {
                String zipFilePath = findZipFile(subFile);
                if (zipFilePath != null)
                    return zipFilePath;
            } else {
                if (subFile.getName().endsWith(".zip")) {
                    return subFile.getAbsolutePath();
                }
            }
        }
        return null;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public EnvelopeResponse<DiagStatusResponse> getExtractorStatus(String uuid) {
        DiagStatusResponse exception = exceptionMap.getIfPresent(uuid);
        if (exception != null) {
            exception.setUuid(uuid);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, exception, "");
        }
        val extractor = extractorMap.getIfPresent(uuid);
        if (extractor == null) {
            throw new RuntimeException("ID {" + uuid + "} is not exist");
        }
        DiagStatusResponse response = new DiagStatusResponse();
        response.setUuid(uuid);
        response.setStatus("000");
        response.setStage(extractor.getStage());
        response.setProgress(extractor.getProgress());
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, response, "");
    }
}
