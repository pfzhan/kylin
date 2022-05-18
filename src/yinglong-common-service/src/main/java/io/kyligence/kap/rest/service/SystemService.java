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

import static io.kyligence.kap.tool.constant.DiagTypeEnum.FULL;
import static io.kyligence.kap.tool.constant.DiagTypeEnum.JOB;
import static io.kyligence.kap.tool.constant.DiagTypeEnum.QUERY;
import static io.kyligence.kap.tool.constant.StageEnum.DONE;
import static org.apache.kylin.common.exception.ServerErrorCode.DIAG_FAILED;
import static org.apache.kylin.common.exception.ServerErrorCode.DIAG_UUID_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_NOT_EXIST;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.transaction.MessageSynchronization;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.rest.request.BackupRequest;
import io.kyligence.kap.rest.request.DiagProgressRequest;
import io.kyligence.kap.rest.response.DiagStatusResponse;
import io.kyligence.kap.tool.MetadataTool;
import io.kyligence.kap.tool.constant.DiagTypeEnum;
import io.kyligence.kap.tool.constant.StageEnum;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

@Service("systemService")
public class SystemService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(SystemService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    @Data
    @NoArgsConstructor
    public static class DiagInfo {
        private final long startTime = System.currentTimeMillis();
        private String stage = StageEnum.PREPARE.toString();
        private float progress = 0.0f;
        private File exportFile;
        private Future task;
        private DiagTypeEnum diagType;
        private long updateTime;

        public DiagInfo(File exportFile, Future task, DiagTypeEnum diagType) {
            this.exportFile = exportFile;
            this.task = task;
            this.diagType = diagType;
        }
    }

    private Cache<String, DiagInfo> diagMap = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS).build();
    private Cache<String, DiagStatusResponse> exceptionMap = CacheBuilder.newBuilder()
            .expireAfterAccess(1, TimeUnit.DAYS).build();
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#backupRequest.getProject(), 'ADMINISTRATION')")
    public void backup(BackupRequest backupRequest) throws Exception {
        String[] args = createBackupArgs(backupRequest);
        val metadataTool = new MetadataTool(getConfig());
        metadataTool.execute(args);
    }

    private String[] createBackupArgs(BackupRequest backupRequest) {
        List<String> args = Lists.newArrayList("-backup");
        if (backupRequest.isCompress()) {
            args.add("-compress");
        }
        if (StringUtils.isNotBlank(backupRequest.getBackupPath())) {
            args.add("-dir");
            args.add(backupRequest.getBackupPath());
        }
        if (StringUtils.isNotBlank(backupRequest.getProject())) {
            args.add("-project");
            args.add(backupRequest.getProject());
        }

        logger.info("SystemService {}", args);
        return args.toArray(new String[0]);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String dumpLocalDiagPackage(String startTime, String endTime, String jobId, String queryId, String project) {
        File exportFile = KylinConfigBase.getDiagFileName();
        String uuid = exportFile.getName();
        FileUtils.deleteQuietly(exportFile);
        exportFile.mkdirs();

        CliCommandExecutor commandExecutor = new CliCommandExecutor();
        val patternedLogger = new BufferedLogger(logger);

        DiagTypeEnum diagPackageType;
        String[] arguments;
        // full
        if (StringUtils.isEmpty(jobId) && StringUtils.isEmpty(queryId)) {
            if (startTime == null && endTime == null) {
                startTime = Long.toString(System.currentTimeMillis() - 259200000L);
                endTime = Long.toString(System.currentTimeMillis());
            }
            arguments = new String[] { "-destDir", exportFile.getAbsolutePath(), "-startTime", startTime, "-endTime",
                    endTime, "-diagId", uuid };
            diagPackageType = FULL;
        } else if (StringUtils.isEmpty(queryId)) {//job
            String jobOpt = "-job";
            if (StringUtils.endsWithAny(jobId, new String[] { "_build", "_merge" })) {
                jobOpt = "-streamingJob";
            }
            arguments = new String[] { jobOpt, jobId, "-destDir", exportFile.getAbsolutePath(), "-diagId", uuid };
            diagPackageType = JOB;
        } else { //query
            arguments = new String[] { "-project", project, "-query", queryId, "-destDir", exportFile.getAbsolutePath(), "-diagId", uuid };
            diagPackageType = QUERY;
        }
        Future<?> task = executorService.submit(() -> {
            try {
                exceptionMap.invalidate(uuid);
                String finalCommand = String.format(Locale.ROOT, "%s/bin/diag.sh %s", KylinConfig.getKylinHome(),
                        StringUtils.join(arguments, " "));
                commandExecutor.execute(finalCommand, patternedLogger, uuid);

                DiagInfo diagInfo = diagMap.getIfPresent(uuid);
                if (Objects.isNull(diagInfo) || !"DONE".equals(diagInfo.getStage())) {
                    throw new KylinException(DIAG_FAILED, MsgPicker.getMsg().getDiagFailed());
                }
            } catch (Exception ex) {
                handleDiagException(uuid, ex);
            }
        });
        diagMap.put(uuid, new DiagInfo(exportFile, task, diagPackageType));
        return uuid;
    }

    public String dumpLocalQueryDiagPackage(String queryId, String project) {
        if (!KylinConfig.getInstanceFromEnv().isAllowedNonAdminGenerateQueryDiagPackage()) {
            aclEvaluate.checkIsGlobalAdmin();
        }
        return dumpLocalDiagPackage(null, null, null, queryId, project);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String dumpLocalDiagPackage(String startTime, String endTime, String jobId) {
        return dumpLocalDiagPackage(startTime, endTime, jobId, null, null);
    }

    private void handleDiagException(String uuid, @NotNull Exception ex) {
        logger.warn("Diagnostic kit error", ex);
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
        DiagInfo diagInfo = diagMap.getIfPresent(uuid);
        if (diagInfo != null) {
            response.setDuration(System.currentTimeMillis() - diagInfo.getStartTime());
        }
        exceptionMap.put(uuid, response);
        FileUtils.deleteQuietly(diagInfo == null ? null : diagInfo.getExportFile());
        diagMap.invalidate(uuid);
    }

    public String getDiagPackagePath(String uuid) {
        DiagStatusResponse exception = exceptionMap.getIfPresent(uuid);
        if (exception != null) {
            throw new RuntimeException(exception.getError());
        }
        DiagInfo diagInfo = diagMap.getIfPresent(uuid);
        if (diagInfo != null && !"DONE".equals(diagInfo.getStage())) {
            throw new RuntimeException("Diagnostic task is running now , can not download yet");
        }
        File exportFile = diagInfo == null ? null : diagInfo.getExportFile();
        if (exportFile == null) {
            throw new KylinException(DIAG_UUID_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getInvalidId(), uuid));
        }
        if (QUERY != diagInfo.getDiagType() || !KylinConfig.getInstanceFromEnv().isAllowedNonAdminGenerateQueryDiagPackage()) {
            aclEvaluate.checkIsGlobalAdmin();
        }
        String zipFilePath = findZipFile(exportFile);
        if (zipFilePath == null) {
            throw new KylinException(FILE_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getDiagPackageNotAvailable(), exportFile.getAbsoluteFile()));
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

    public EnvelopeResponse<DiagStatusResponse> getExtractorStatus(String uuid) {
        DiagStatusResponse exception = exceptionMap.getIfPresent(uuid);
        if (exception != null) {
            exception.setUuid(uuid);
            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, exception, "");
        }
        DiagInfo diagInfo = diagMap.getIfPresent(uuid);
        if (Objects.isNull(diagInfo)) {
            throw new KylinException(DIAG_UUID_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getInvalidId(), uuid));
        }
        if (QUERY != diagInfo.getDiagType() || !KylinConfig.getInstanceFromEnv().isAllowedNonAdminGenerateQueryDiagPackage()) {
            aclEvaluate.checkIsGlobalAdmin();
        }
        DiagStatusResponse response = new DiagStatusResponse();
        response.setUuid(uuid);
        response.setStatus("000");
        response.setStage(diagInfo.getStage());
        response.setProgress(diagInfo.getProgress());
        long endTime = System.currentTimeMillis();
        if (DONE.toString().equals(diagInfo.getStage())) {
            endTime = diagInfo.getUpdateTime();
        }
        response.setDuration(endTime - diagInfo.startTime);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, response, "");
    }

    public void updateDiagProgress(DiagProgressRequest diagProgressRequest) {
        DiagInfo diagInfo = diagMap.getIfPresent(diagProgressRequest.getDiagId());
        if (Objects.isNull(diagInfo)) {
            throw new KylinException(DIAG_UUID_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getInvalidId(), diagProgressRequest.getDiagId()));
        }
        diagInfo.setStage(diagProgressRequest.getStage());
        diagInfo.setProgress(diagProgressRequest.getProgress());
        diagInfo.setUpdateTime(diagProgressRequest.getUpdateTime());
    }

    public void stopDiagTask(String uuid) {
        logger.debug("Stop diagnostic package task {}", uuid);
        DiagInfo diagInfo = diagMap.getIfPresent(uuid);
        if (diagInfo == null) {
            throw new KylinException(DIAG_UUID_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getInvalidId(), uuid));
        }
        if (QUERY != diagInfo.getDiagType() || !KylinConfig.getInstanceFromEnv().isAllowedNonAdminGenerateQueryDiagPackage()) {
            aclEvaluate.checkIsGlobalAdmin();
        }
        EventBusFactory.getInstance().postSync(new CliCommandExecutor.JobKilled(uuid));
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void reloadMetadata() throws IOException {
            MessageSynchronization messageSynchronization = MessageSynchronization.getInstance(KylinConfig.getInstanceFromEnv());
            messageSynchronization.replayAllMetadata(true);
    }
}
