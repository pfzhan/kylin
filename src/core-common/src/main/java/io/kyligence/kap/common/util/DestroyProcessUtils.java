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

package io.kyligence.kap.common.util;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.JobProcessContext;
import org.apache.kylin.common.KylinConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DestroyProcessUtils {

    private static final String KILL_PROCESS_TREE = "kill-process-tree.sh";
    private static final int CMD_EXEC_TIMEOUT_SEC = 60;

    private DestroyProcessUtils() {
    }

    /**
     * The method is used for job and diag.You need to make sure the jobId can't be repeated.
     *
     * @param jobId we can treat the job id and the diag id as jobId.
     * Job id is uuid, eg: df64cd83-ebab-45ef-b391-c640423c773f
     * Diag id has a certain format, front_yyyy_MM_dd_HH_mm_ss_uuid(0-5) eg: front_2020_08_26_01_55_50_F6025C
     *
     */
    public static void destroyProcessByJobId(String jobId) {
        Process originProc = JobProcessContext.getProcess(jobId);
        if (Objects.nonNull(originProc) && originProc.isAlive()) {
            try {
                final int pid = JobProcessContext.getPid(originProc);
                log.info("Start to destroy process {} of job {}", pid, jobId);
                final String killCmd = String.format("bash %s/sbin/%s %s", KylinConfig.getKylinHome(), KILL_PROCESS_TREE, pid);
                Process killProc = Runtime.getRuntime().exec(killCmd);
                if (killProc.waitFor(CMD_EXEC_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                    log.info("Try to destroy process {} of job {}, exec cmd '{}', exitValue : {}", pid, jobId,
                            killCmd, killProc.exitValue());
                    if (!originProc.isAlive()) {
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
        }
    }

}
