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

package io.kyligence.kap.query.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.status.api.v1.ExecutorSummary;
import org.apache.spark.status.api.v1.StageData;
import org.apache.spark.status.api.v1.StageStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.scheduler.EventBusFactory;
import lombok.val;
import scala.collection.JavaConverters;

public class LoadCounter {

    private static final long PERIOD_SECONDS = KylinConfig.getInstanceFromEnv().getLoadCounterPeriodSeconds();
    private static final Logger logger = LoggerFactory.getLogger(LoadCounter.class);

    private CircularFifoQueue<Integer> pendingQueue = new CircularFifoQueue(
            KylinConfig.getInstanceFromEnv().getLoadCounterCapacity());

    public static LoadCounter getInstance() {
        return Singletons.getInstance(LoadCounter.class, v -> new LoadCounter());
    }

    public void init(ScheduledExecutorService executorService) {
        logger.info("Start load pending task");
        executorService.scheduleWithFixedDelay(this::fetchTaskCount, 20, PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    void fetchTaskCount() {
        try {
            val activeStage = SparderEnv.getSparkSession().sparkContext().statusStore().activeStages();
            val pendingTaskCount = JavaConverters.seqAsJavaList(activeStage).stream()
                    .filter(stage -> StageStatus.ACTIVE == stage.status())
                    .map(stageData -> stageData.numTasks() - stageData.numActiveTasks() - stageData.numCompleteTasks())
                    .mapToInt(i -> i).sum();
            val activeTaskCount = JavaConverters.seqAsJavaList(activeStage).stream()
                    .filter(stage -> StageStatus.ACTIVE == stage.status()).map(StageData::numActiveTasks)
                    .mapToInt(i -> i).sum();
            val finishedTaskCount = JavaConverters.seqAsJavaList(activeStage).stream()
                    .filter(stage -> StageStatus.ACTIVE == stage.status()).map(StageData::numCompleteTasks)
                    .mapToInt(i -> i).sum();
            pendingQueue.add(pendingTaskCount);
            EventBusFactory.getInstance()
                    .postAsync(new TaskStatusEvent(pendingTaskCount, activeTaskCount, finishedTaskCount));
        } catch (Exception ex) {
            logger.error("Error when fetch spark pending task", ex);
        }
    }

    public LoadDesc getLoadDesc() {
        val points = new ArrayList<>(pendingQueue);
        logger.trace("Points is {}", points);
        val mean = median(points);
        val executorSummary = SparderEnv.getSparkSession().sparkContext().statusStore().executorList(true);
        val coreNum = JavaConverters.seqAsJavaList(executorSummary).stream().map(ExecutorSummary::totalCores)
                .mapToInt(i -> i).sum();
        val loadDesc = new LoadDesc(mean / coreNum, coreNum, points);
        logger.debug("LoadDesc is {}", loadDesc);
        return loadDesc;
    }

    public int getSlotCount() {
        return SparderEnv.getTotalCore();
    }

    private static double median(List<Integer> total) {
        double j;
        Collections.sort(total);
        int size = total.size();
        if (size == 0) {
            return 0;
        }
        if (size % 2 == 1) {
            j = total.get((size - 1) / 2);
        } else {
            j = (total.get(size / 2 - 1) + total.get(size / 2) + 0.0) / 2;
        }
        return j;
    }

}