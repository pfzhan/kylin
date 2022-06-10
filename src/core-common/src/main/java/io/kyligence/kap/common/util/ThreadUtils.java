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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * methods copy from 'io.kyligence.kap.engine.spark.utils.ThreadUtils', to fix module cycle dependency
 */
public class ThreadUtils {

    private static final String NAME_SUFFIX = "-%d";

    public static ThreadFactory newDaemonThreadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build();
    }

    public static ScheduledExecutorService newDaemonSingleThreadScheduledExecutor(String threadName) {
        ThreadFactory factory = newDaemonThreadFactory(threadName);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, factory);
        // By default, a cancelled task is not automatically removed from the work queue until its delay
        // elapses. We have to enable it manually.
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    public static ScheduledExecutorService newDaemonThreadScheduledExecutor(int corePoolSize, String threadName) {
        ThreadFactory factory = newDaemonThreadFactory(threadName);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(corePoolSize, factory);
        // By default, a cancelled task is not automatically removed from the work queue until its delay
        // elapses. We have to enable it manually.
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    public static ThreadPoolExecutor newDaemonScalableThreadPool(String prefix, int corePoolSize, //
            int maximumPoolSize, //
            long keepAliveTime, //
            TimeUnit unit) {
        // Why not general BlockingQueue like LinkedBlockingQueue?
        // If there are more than corePoolSize but less than maximumPoolSize threads running,
        //  a new thread will be created only if the queue is full.
        // If we use unbounded queue, then maximumPoolSize will never be used.
        LinkedTransferQueue queue = new LinkedTransferQueue<Runnable>() {
            @Override
            public boolean offer(Runnable r) {
                return tryTransfer(r);
            }
        };
        ThreadFactory factory = newDaemonThreadFactory(prefix + NAME_SUFFIX);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, //
                keepAliveTime, unit, queue, factory);
        threadPool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                try {
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        return threadPool;
    }

}
