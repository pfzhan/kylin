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

package io.kyligence.kap.engine.spark.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;

public class BuildLayoutWithUpdate {
    protected static final Logger logger = LoggerFactory.getLogger(BuildLayoutWithUpdate.class);
    private ExecutorService pool = Executors.newCachedThreadPool();
    private CompletionService<JobResult> completionService = new ExecutorCompletionService<>(pool);
    private int currentLayoutsNum = 0;

    private final ScheduledExecutorService checkPointer;
    private static final int CHECK_POINT_DELAY_SEC = 5;
    private final LinkedBlockingQueue<LayoutPoint> layoutPoints;

    public BuildLayoutWithUpdate() {
        layoutPoints = new LinkedBlockingQueue<>();
        checkPointer = Executors.newSingleThreadScheduledExecutor();
        startCheckPoint();
    }

    public void submit(JobEntity job, KylinConfig config) {
        completionService.submit(new Callable<JobResult>() {
            @Override
            public JobResult call() throws Exception {
                KylinConfig.setAndUnsetThreadLocalConfig(config);
                Thread.currentThread().setName("thread-" + job.getName());
                List<NDataLayout> nDataLayouts = new LinkedList<>();
                Throwable throwable = null;
                try {
                    nDataLayouts = job.build();
                } catch (Throwable t) {
                    logger.error("Error occurred when run " + job.getName(), t);
                    throwable = t;
                }
                return new JobResult(job.getIndexId(), nDataLayouts, throwable);
            }
        });
        currentLayoutsNum++;
    }

    public void updateLayout(NDataSegment seg, KylinConfig config, String project) {
        for (int i = 0; i < currentLayoutsNum; i++) {
            updateSingleLayout(seg, config, project);
        }
        // flush 
        doCheckPoint();
        currentLayoutsNum = 0;
    }

    public long updateSingleLayout(NDataSegment seg, KylinConfig config, String project) {
        long indexId = -1l;
        try {
            logger.info("Wait to take job result.");
            JobResult result = completionService.take().get();
            logger.info("Take job result successful.");
            if (result.isFailed()) {
                shutDown();
                throw new RuntimeException(result.getThrowable());
            }
            indexId = result.getIndexId();
            for (NDataLayout layout : result.getLayouts()) {
                logger.info("Update layout {} in dataflow {}, segment {}", layout.getLayoutId(),
                        seg.getDataflow().getUuid(), seg.getId());
                if (!layoutPoints.offer(new LayoutPoint(project, config, seg.getDataflow().getId(), layout))) {
                    throw new IllegalStateException(
                            "[UNLIKELY_THINGS_HAPPENED] Make sure that layoutPoints can offer.");
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            shutDown();
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        // flush 
        doCheckPoint();
        return indexId;
    }

    private void startCheckPoint() {
        checkPointer.scheduleWithFixedDelay(this::doCheckPoint, CHECK_POINT_DELAY_SEC, CHECK_POINT_DELAY_SEC,
                TimeUnit.SECONDS);
    }

    private synchronized void doCheckPoint() {
        LayoutPoint lp = layoutPoints.poll();
        if (Objects.isNull(lp)) {
            return;
        }

        if (Objects.isNull(lp.getLayout())) {
            logger.warn("[LESS_LIKELY_THINGS_HAPPENED] layout shouldn't be empty.");
            return;
        }

        final String project = lp.getProject();
        final KylinConfig config = lp.getConfig();
        final String dataFlowId = lp.getDataFlowId();
        final List<NDataLayout> layouts = new ArrayList<>();
        layouts.add(lp.getLayout());
        StringBuilder sb = new StringBuilder("Checkpoint layouts: ").append(lp.getLayout().getLayoutId());

        while (Objects.nonNull(lp = layoutPoints.peek()) && Objects.equals(project, lp.getProject())
                && Objects.equals(dataFlowId, lp.getDataFlowId()) && Objects.nonNull(lp.getLayout())) {
            layouts.add(lp.getLayout());
            sb.append(',').append(lp.getLayout().getLayoutId());
            LayoutPoint temp = layoutPoints.remove();
            if (!Objects.equals(lp, temp)) {
                throw new IllegalStateException(
                        "[UNLIKELY_THINGS_HAPPENED] Make sure that only one thread can execute layoutPoints poll.");
            }
        }

        // add log
        final String layoutsLog = sb.toString();
        logger.info(layoutsLog);

        // persist to storage
        KylinConfig.setAndUnsetThreadLocalConfig(config);
        updateLayouts(config,project,dataFlowId,layouts);

        if (Objects.nonNull(layoutPoints.peek())) {
            // schedule immediately
            doCheckPoint();
        }
    }

    protected void updateLayouts(KylinConfig config,String project, String dataFlowId,
                                 final List<NDataLayout> layouts) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowUpdate update = new NDataflowUpdate(dataFlowId);
            update.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflow(update);
            return 0;
        }, project);
    }

    public void shutDown() {
        pool.shutdown();
        try {
            pool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Error occurred when shutdown thread pool.", e);
            ExecutorServiceUtil.forceShutdown(pool);
            Thread.currentThread().interrupt();
        }

        checkPointer.shutdown();
        try {
            checkPointer.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("Error occurred when shutdown checkPointer.", e);
            ExecutorServiceUtil.forceShutdown(checkPointer);
            Thread.currentThread().interrupt();
        }
    }

    private static class LayoutPoint {
        private final String project;
        private final KylinConfig config;
        private final String dataFlowId;
        private final NDataLayout layout;

        public LayoutPoint(String project, KylinConfig config, String dataFlowId, NDataLayout layout) {
            this.project = project;
            this.config = config;
            this.dataFlowId = dataFlowId;
            this.layout = layout;
        }

        public String getProject() {
            return project;
        }

        public KylinConfig getConfig() {
            return config;
        }

        public String getDataFlowId() {
            return dataFlowId;
        }

        public NDataLayout getLayout() {
            return layout;
        }
    }

    private static class JobResult {
        private long indexId;
        private List<NDataLayout> layouts;
        private Throwable throwable;

        JobResult(long indexId, List<NDataLayout> layouts, Throwable throwable) {
            this.indexId = indexId;
            this.layouts = layouts;
            this.throwable = throwable;
        }

        boolean isFailed() {
            return throwable != null;
        }

        Throwable getThrowable() {
            return throwable;
        }

        long getIndexId() {
            return indexId;
        }

        List<NDataLayout> getLayouts() {
            return layouts;
        }
    }

    public static abstract class JobEntity {

        public abstract long getIndexId();

        public abstract String getName();

        public abstract List<NDataLayout> build() throws IOException;
    }
}
