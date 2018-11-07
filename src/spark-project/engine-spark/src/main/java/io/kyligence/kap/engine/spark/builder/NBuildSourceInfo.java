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

package io.kyligence.kap.engine.spark.builder;

import java.util.LinkedHashSet;
import java.util.Set;

import io.kyligence.kap.cube.model.NDataSegment;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.kyligence.kap.cube.model.NCuboidDesc;

public class NBuildSourceInfo {
    private Dataset<Row> dataset;
    private long byteSize;
    private long count;
    private long layoutId;
    private Set<NCuboidDesc> toBuildCuboids = new LinkedHashSet<>();
    private NDataSegment segment;

    public long getByteSize() {
        return byteSize;
    }

    public void setByteSize(long byteSize) {
        this.byteSize = byteSize;
    }

    public void setDataset(Dataset<Row> dataset) {
        this.dataset = dataset;
    }

    public Dataset<Row> getDataset() {
        return dataset;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public void setLayoutId(long layoutId) {
        this.layoutId = layoutId;
    }

    public long getLayoutId() {
        return layoutId;
    }

    public void setToBuildCuboids(Set<NCuboidDesc> toBuildCuboids) {
        this.toBuildCuboids = toBuildCuboids;
    }

    public Set<NCuboidDesc> getToBuildCuboids() {
        return this.toBuildCuboids;
    }

    public void addCuboid(NCuboidDesc cuboid) {
        this.toBuildCuboids.add(cuboid);
    }

    public NDataSegment getSegment() {
        return segment;
    }

    public void setSegment(NDataSegment segment) {
        this.segment = segment;
    }
}