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
package io.kyligence.kap.storage.parquet;

import scala.Serializable;

public class RDDPartitionResult implements Serializable {

    private byte[] data;
    private long scannedRows;
    private long scannedBytes;
    private long returnedRows;
    private String hostname;
    private long startLatency;
    private long totalDuration;

    public RDDPartitionResult(byte[] data, long scannedRows, long scannedBytes, long returnedRows,
        String hostname, long startLatency, long totalDuration) {
        this.data = data;
        this.scannedRows = scannedRows;
        this.scannedBytes = scannedBytes;
        this.returnedRows = returnedRows;
        this.hostname = hostname;
        this.startLatency = startLatency;
        this.totalDuration = totalDuration;
    }

    public byte[] getData() {
        return data;
    }

    public long getScannedRows() {
        return scannedRows;
    }

    public long getScannedBytes() {
        return scannedBytes;
    }

    public long getReturnedRows() {
        return returnedRows;
    }

    public String getHostname() {
        return hostname;
    }

    public long getStartLatency() {
        return startLatency;
    }

    public long getTotalDuration() {
        return totalDuration;
    }
}
