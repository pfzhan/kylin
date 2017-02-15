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

import java.util.List;

public class SparkDriverClientParams {
    private final String kylinProperties;
    private final String realizationType;
    private final String realizationId;
    private final String segmentId;
    private final String cuboidId;
    private final int maxGTLength;
    private final List<Integer> parquetColumns;
    private final boolean useII;
    private final String queryId;
    private final boolean spillEnabled;
    private final long partitionMaxScanBytes;

    public SparkDriverClientParams(String kylinProperties, String realizationType, String realizationId, String segmentId, String cuboidId, int maxGTLength, //
            List<Integer> parquetColumns, boolean useII, String queryId, boolean spillEnabled, long partitionMaxScanBytes) {
        this.kylinProperties = kylinProperties;
        this.realizationType = realizationType;
        this.realizationId = realizationId;
        this.segmentId = segmentId;
        this.cuboidId = cuboidId;
        this.maxGTLength = maxGTLength;
        this.parquetColumns = parquetColumns;
        this.useII = useII;
        this.queryId = queryId == null ? "" : queryId;
        this.spillEnabled = spillEnabled;
        this.partitionMaxScanBytes = partitionMaxScanBytes;
    }

    public String getKylinProperties() {
        return kylinProperties;
    }

    public String getRealizationId() {
        return realizationId;
    }

    public String getSegmentId() {
        return segmentId;
    }

    public String getCuboidId() {
        return cuboidId;
    }

    public int getMaxGTLength() {
        return maxGTLength;
    }

    public List<Integer> getParquetColumns() {
        return parquetColumns;
    }

    public String getRealizationType() {
        return realizationType;
    }

    public boolean isUseII() {
        return useII;
    }

    public String getQueryId() {
        return queryId;
    }

    public boolean isSpillEnabled() {
        return spillEnabled;
    }

    public long getPartitionMaxScanBytes() {
        return partitionMaxScanBytes;
    }
}
