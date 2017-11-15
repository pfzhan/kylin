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
package io.kyligence.kap.storage.parquet.cube.spark.rpc;

import org.apache.htrace.TraceInfo;

import java.util.concurrent.SynchronousQueue;

public class StorageVisitState {

    public enum ResultPackStatus {
        NORMAL, LAST, ERROR
    }

    public static class TransferPack {
        public ResultPackStatus status;
        public RDDPartitionResult rddPartitionResult;//for ResultPackStatus.NORMAL and ResultPackStatus.LAST
        public Throwable visitThreadFailCause;//for ResultPackStatus.ERROR

        private TransferPack() {
        }

        public static TransferPack createNormalPack(RDDPartitionResult result) {
            TransferPack ret = new TransferPack();
            ret.status = ResultPackStatus.NORMAL;
            ret.rddPartitionResult = result;
            return ret;
        }

        public static TransferPack createLastPack(RDDPartitionResult result) {
            TransferPack ret = new TransferPack();
            ret.status = ResultPackStatus.LAST;
            ret.rddPartitionResult = result;
            return ret;
        }

        public static TransferPack createErrorPack(Throwable throwable) {
            TransferPack ret = new TransferPack();
            ret.status = ResultPackStatus.ERROR;
            ret.visitThreadFailCause = throwable;
            return ret;
        }
    }

    private volatile SynchronousQueue<TransferPack> synchronousQueue = new SynchronousQueue<>();

    private volatile long createTime = System.currentTimeMillis();
    private volatile int messageNum = -1;//a complete storage visit may contain multiple streams
    private volatile boolean userCanceled = false;
    private volatile TraceInfo traceInfo = null;

    public SynchronousQueue<TransferPack> getSynchronousQueue() {
        return synchronousQueue;
    }

    public TraceInfo getTraceInfo() {
        return traceInfo;
    }

    public void setTraceInfo(TraceInfo traceInfo) {
        this.traceInfo = traceInfo;
    }

    public boolean isUserCanceled() {
        return userCanceled;
    }

    public void setUserCanceled(boolean userCanceled) {
        this.userCanceled = userCanceled;
    }

    public long getCreateTime() {
        return createTime;
    }

    public int incAndGetMessageNum() {
        return ++this.messageNum;
    }

}
