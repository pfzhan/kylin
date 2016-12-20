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

import java.util.concurrent.SynchronousQueue;

public class StorageVisitState {

    private volatile boolean noMore = false;
    private volatile SynchronousQueue<SparkParquetVisit.RDDPartitionData> synchronousQueue = new SynchronousQueue<>();

    private volatile long createTime = System.currentTimeMillis();
    private volatile int messageNum = -1;//a complete storage visit may contain multiple streams
    private volatile boolean userCanceled = false;
    private volatile Throwable visitThreadFailCause;

    public SynchronousQueue<SparkParquetVisit.RDDPartitionData> getSynchronousQueue() {
        return synchronousQueue;
    }

    public boolean isUserCanceled() {
        return userCanceled;
    }

    public void setUserCanceled(boolean userCanceled) {
        this.userCanceled = userCanceled;
    }

    public boolean isVisitThreadFailed() {
        return visitThreadFailCause != null;
    }

    public Throwable getVisitThreadFailCause() {
        return visitThreadFailCause;
    }

    public void setVisitThreadFailCause(Throwable visitThreadFailCause) {
        this.visitThreadFailCause = visitThreadFailCause;
    }

    public long getCreateTime() {
        return createTime;
    }

    public int incAndGetMessageNum() {
        return ++this.messageNum;
    }

    public boolean isNoMore() {
        return noMore;
    }

    public void setNoMore(boolean noMore) {
        this.noMore = noMore;
    }
}
