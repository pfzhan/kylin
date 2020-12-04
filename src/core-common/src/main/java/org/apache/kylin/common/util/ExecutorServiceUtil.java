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

package org.apache.kylin.common.util;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

//https://www.baeldung.com/java-executor-wait-for-threads
public class ExecutorServiceUtil {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorServiceUtil.class);

    public static void shutdownGracefully(ExecutorService threadPool, int timeoutSeconds) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
                forceShutdown(threadPool);
            }
        } catch (InterruptedException ex) {
            logger.info("Interrupted while shutting down");
            forceShutdown(threadPool);
            Thread.currentThread().interrupt();
        }
    }

    public static void forceShutdown(ExecutorService threadPool) {
        if (threadPool != null) {
            List<Runnable> jobs = threadPool.shutdownNow();
            logger.info("Shutdown now thread pool [{}], drain [{}] runnable jobs.", System.identityHashCode(threadPool),
                    jobs.size());
            int futureTaskCount = 0;
            for (Runnable job : jobs) {
                if (job instanceof Future) {
                    futureTaskCount++;
                    val task = (Future) job;
                    logger.info("Cancel future task [{}].", System.identityHashCode(task));
                    task.cancel(true);
                }
            }
            logger.info("Thread poll cancel [{}] future jobs.", futureTaskCount);
        }
    }
}
