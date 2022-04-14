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
package io.kyligence.kap.rest.config.initialize;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.ProcessUtils;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProcessStatusListener {

    private static final File CHILD_PROCESS_FILE = new File(KapConfig.getKylinHomeAtBestEffort(), "child_process");
    private static final String KILL_PROCESS_TREE = "kill-process-tree.sh";
    private static final int CMD_EXEC_TIMEOUT_SEC = 60;

    // Lock subscribed actions which would read or write the child-process file.
    private final Lock fileLock = new ReentrantLock();

    @Subscribe
    public void onProcessStart(CliCommandExecutor.ProcessStart processStart) {
        int pid = processStart.getPid();
        fileLock.lock();
        try (OutputStream os = new FileOutputStream(CHILD_PROCESS_FILE, true);
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(os, Charset.defaultCharset().name()))) {
            writer.write(pid + "," + processStart.getJobId() + "\n");
            writer.flush();
        } catch (IOException ex) {
            log.error("write child job process {} from {} failed", pid, CHILD_PROCESS_FILE.getAbsolutePath());
        } finally {
            fileLock.unlock();
        }
    }

    @Subscribe
    public void onProcessFinished(CliCommandExecutor.ProcessFinished processFinished) {
        int pid = processFinished.getPid();
        removeProcessPidInChildProcess(pid);
    }

    private void removeProcessPidInChildProcess(int pid){
        if (!CHILD_PROCESS_FILE.exists())
            return;

        fileLock.lock();
        try {
            val children = parseProcessFile();

            if (!children.containsKey(pid))
                return;

            children.remove(pid);
            persistProcessFile(children);
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * The method is used for job and diag.You need to make sure the jobId can't be repeated.
     *
     * Job id is uuid, eg: df64cd83-ebab-45ef-b391-c640423c773f
     * Diag id has a certain format, front_yyyy_MM_dd_HH_mm_ss_uuid(0-5) eg: front_2020_08_26_01_55_50_F6025C
     *
     */
    @Subscribe
    public void destroyProcessByJobId(CliCommandExecutor.JobKilled jobKilled) {
        val jobId = jobKilled.getJobId();
        final Map<Integer, String> children;
        fileLock.lock();
        try {
            children = parseProcessFile();
        } finally {
            fileLock.unlock();
        }
        Optional<Integer> maybePid = children.entrySet().stream().filter(entry -> entry.getValue().equals(jobId))
                .map(Map.Entry::getKey).findAny();
        if (!maybePid.isPresent()) {
            log.info("Cannot find pid for job:<{}>", jobId);
            return;
        }
        int pid = maybePid.get();
        log.debug("Try to kill process {}", pid);
        if (ProcessUtils.isAlive(pid)) {
            try {
                log.info("Start to destroy process {} of job {}", pid, jobId);
                final String killCmd = String.format(Locale.ROOT, "bash %s/sbin/%s %s", KylinConfig.getKylinHome(),
                        KILL_PROCESS_TREE, pid);
                Process killProc = Runtime.getRuntime().exec(killCmd);
                if (killProc.waitFor(CMD_EXEC_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                    log.info("Try to destroy process {} of job {}, exec cmd '{}', exitValue : {}", pid, jobId, killCmd,
                            killProc.exitValue());
                    if (!ProcessUtils.isAlive(pid)) {
                        log.info("Destroy process {} of job {} SUCCEED.", pid, jobId);
                        return;
                    }
                    log.info("Destroy process {} of job {} FAILED.", pid, jobId);
                } else {
                    log.warn("Destroy process {} of job {} TIMEOUT exceed {}s.", pid, jobId, CMD_EXEC_TIMEOUT_SEC);
                }
            } catch (Exception e) {
                log.error("Destroy process of job {} FAILED.", jobId, e);
            }
        } else {
            log.info("Ignore not alive process {} of job {}", pid, jobId);
            removeProcessPidInChildProcess(pid);
        }
    }

    private void persistProcessFile(Map<Integer, String> children) {
        try {
            FileUtils.writeLines(CHILD_PROCESS_FILE, children.entrySet().stream()
                    .map(entry -> entry.getKey() + "," + entry.getValue()).collect(Collectors.toList()));
        } catch (IOException e) {
            log.error("persist child_process failed, expected status is {}", children);
        }
    }

    @VisibleForTesting
    static Map<Integer, String> parseProcessFile() {
        Map<Integer, String> result = Maps.newHashMap();
        try {
            for (String line : FileUtils.readLines(CHILD_PROCESS_FILE)) {
                val elements = line.split(",");
                result.put(Integer.parseInt(elements[0]), elements[1]);
            }
        } catch (IOException e) {
            log.error("read child job process from {} failed", CHILD_PROCESS_FILE.getAbsolutePath());
        }
        return result;
    }

}
