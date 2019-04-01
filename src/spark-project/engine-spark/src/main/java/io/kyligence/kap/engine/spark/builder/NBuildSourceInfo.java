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

import java.util.Collection;
import java.util.LinkedHashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.model.IndexEntity;

public class NBuildSourceInfo {
    private Dataset<Row> flattableDS;
    private SparkSession ss;
    private long byteSize;
    private long count;
    private long layoutId;
    private String parentStoragePath;
    private Collection<IndexEntity> toBuildCuboids = new LinkedHashSet<>();

    public long getByteSize() {
        return byteSize;
    }

    public void setByteSize(long byteSize) {
        this.byteSize = byteSize;
    }

    public void setFlattableDS(Dataset<Row> flattableDS) {
        this.flattableDS = flattableDS;
    }

    public Dataset<Row> getParentDS() {
        if (flattableDS != null) {
            return flattableDS;
        } else {
            Preconditions.checkState(!StringUtils.isBlank(parentStoragePath), "Parent storage path is blank.");
            Preconditions.checkNotNull(ss, "SparkSession is null is NBuildSourceInfo.");
            return ss.read().parquet(parentStoragePath);
        }
    }

    public void setSparkSession(SparkSession ss) {
        this.ss = ss;
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

    public void setToBuildCuboids(Collection<IndexEntity> toBuildCuboids) {
        this.toBuildCuboids = toBuildCuboids;
    }

    public Collection<IndexEntity> getToBuildCuboids() {
        return this.toBuildCuboids;
    }

    public void addCuboid(IndexEntity cuboid) {
        this.toBuildCuboids.add(cuboid);
    }

    public void setParentStoragePath(String parentStoragePath) {
        this.parentStoragePath = parentStoragePath;
    }
}