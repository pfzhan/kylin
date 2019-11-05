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
package io.kyligence.kap.rest.controller;

import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.TaskSchedulerImpl;
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend;
import org.apache.spark.sql.SparderEnv;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import scala.Tuple2;

@Controller
@RequestMapping(value = "/spark")
public class NSparkController extends NBasicController {

    @RequestMapping(value = "/blacklist", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<Tuple2<String[], String[]>> getBlacklist() {
        Tuple2<String[], String[]> blacklist = getSparkTaskScheduler().getBlacklist();
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, blacklist, "get blacklist");
    }

    @RequestMapping(value = "/blacklist/executor/{executorId}", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> addExecutorToBlackListManually(@PathVariable String executorId) {
        if (getBackend().getExecutorIds().contains(executorId)) {
            String host = getBackend().getHostByExecutor(executorId);
            if (!host.isEmpty()) {
                getSparkTaskScheduler().addExecutorToBlackListManually(executorId, host);
                return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "add executor to blacklist");
            } else {
                return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, null,
                        "Can not get host by executor " + executorId);
            }
        } else {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, null,
                    "Executor " + executorId + " not exists in Spark");
        }
    }

    @RequestMapping(value = "/blacklist/node/{node:.+}", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> addNodeToBlackListManually(@PathVariable String node) {
        if (getBackend().getHosts().contains(node)) {
            getSparkTaskScheduler().addNodeToBlackListManually(node);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "add node to blacklist");
        } else {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, null, "Node " + node + " not exists in Spark");
        }
    }

    @RequestMapping(value = "/blacklist/executor/{executorId}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> removeExecutorFromBlackListManually(@PathVariable String executorId) {
        if (getBackend().getExecutorIds().contains(executorId)) {
            getSparkTaskScheduler().removeExecutorFromBlackListManually(executorId);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "del executor from blacklist");
        } else {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, null,
                    "Executor " + executorId + " not exists in Spark");
        }
    }

    @RequestMapping(value = "/blacklist/node/{node:.+}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse<String> removeNodeFromBlackListManually(@PathVariable String node) {
        if (getBackend().getHosts().contains(node)) {
            getSparkTaskScheduler().removeNodeFromBlackListManually(node);
            return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, null, "del node from blacklist");
        } else {
            return new EnvelopeResponse<>(ResponseCode.CODE_UNDEFINED, null, "Node " + node + " not exists in Spark");
        }
    }

    private TaskSchedulerImpl getSparkTaskScheduler() {
        SparkContext sc = SparderEnv.getSparkSession().sparkContext();
        return (TaskSchedulerImpl) sc.taskScheduler();
    }

    private CoarseGrainedSchedulerBackend getBackend() {
        SparkContext sc = SparderEnv.getSparkSession().sparkContext();
        if (!(sc.schedulerBackend() instanceof CoarseGrainedSchedulerBackend)) {
            throw new RuntimeException("Only support CoarseGrainedSchedulerBackend now.");
        }
        return (CoarseGrainedSchedulerBackend) sc.schedulerBackend();
    }
}