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

package io.kyligence.kap.engine.spark.utils;

import java.util.List;

import org.apache.hadoop.fs.ContentSummary;

public class RepartitionHelper {
    private int MB = 1024 * 1024;
    private int shardSize;
    private int repartitionThresholdSize;
    private ContentSummary contentSummary;
    private List<Integer> shardByColumns;

    public RepartitionHelper(int shardSize, int repartitionThresholdSize, ContentSummary contentSummary,
            List<Integer> shardByColumns) {
        this.shardSize = shardSize;
        this.repartitionThresholdSize = repartitionThresholdSize;
        this.contentSummary = contentSummary;
        this.shardByColumns = shardByColumns;
    }

    public boolean needRepartitionForFileSize() {
        return (contentSummary.getLength() * 1.0 / MB / contentSummary.getFileCount()) < repartitionThresholdSize && contentSummary.getFileCount() > 1;
    }

    public boolean needRepartitionForShardByColumns() {
        return shardByColumns != null && !shardByColumns.isEmpty();
    }

    public boolean needRepartition() {
        return needRepartitionForFileSize() || needRepartitionForShardByColumns();
    }

    public int getShardSize() {
        return shardSize;
    }

    public int getRepartitionThresholdSize() {
        return repartitionThresholdSize;
    }

    public ContentSummary getContentSummary() {
        return contentSummary;
    }

    public List<Integer> getShardByColumns() {
        return shardByColumns;
    }

    public int getRepartitionNum() {
        return (int) Math.ceil(contentSummary.getLength() * 1.0 / MB / shardSize);
    }

    public void setShardSize(int shardSize) {
        this.shardSize = shardSize;
    }

    public void setRepartitionThresholdSize(int repartitionThresholdSize) {
        this.repartitionThresholdSize = repartitionThresholdSize;
    }

    public void setContentSummary(ContentSummary contentSummary) {
        this.contentSummary = contentSummary;
    }

    public void setShardByColumns(List<Integer> shardByColumns) {
        this.shardByColumns = shardByColumns;
    }
}
