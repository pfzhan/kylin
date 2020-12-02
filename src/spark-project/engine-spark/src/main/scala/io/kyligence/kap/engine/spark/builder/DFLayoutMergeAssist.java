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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.datasource.storage.StorageStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.LayoutPartition;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;

public class DFLayoutMergeAssist implements Serializable {
    protected static final Logger logger = LoggerFactory.getLogger(DFLayoutMergeAssist.class);
    private static final int DEFAULT_BUFFER_SIZE = 256;
    private LayoutEntity layout;
    private NDataSegment newSegment;
    private List<NDataSegment> toMergeSegments;
    private SparkSession ss;
    final private List<NDataLayout> toMergeCuboids = new ArrayList<>();

    public void setSs(SparkSession ss) {
        this.ss = ss;
    }

    public void setLayout(LayoutEntity layout) {
        this.layout = layout;
    }

    public LayoutEntity getLayout() {
        return this.layout;
    }

    public List<NDataLayout> getCuboids() {
        return this.toMergeCuboids;
    }

    public void addCuboid(NDataLayout cuboid) {
        toMergeCuboids.add(cuboid);
    }

    public void setToMergeSegments(List<NDataSegment> segments) {
        this.toMergeSegments = segments;
    }

    public void setNewSegment(NDataSegment segment) {
        this.newSegment = segment;
    }

    public Dataset<Row> merge() {
        Dataset<Row> mergeDataset = null;
        for (int i = 0; i < toMergeCuboids.size(); i++) {
            NDataLayout nDataLayout = toMergeCuboids.get(i);
            ss.sparkContext().setJobDescription("Union segments layout " + nDataLayout.getLayoutId());
            Dataset<Row> layoutDataset = getLayoutDS(nDataLayout);
            if (mergeDataset == null) {
                mergeDataset = layoutDataset;
            } else
                mergeDataset = mergeDataset.union(layoutDataset);

            ss.sparkContext().setJobDescription(null);
        }
        return mergeDataset;
    }

    private Dataset<Row> getLayoutDS(NDataLayout dataLayout) {
        final NDataSegment dataSegment = dataLayout.getSegDetails().getDataSegment();
        if (CollectionUtils.isEmpty(dataLayout.getMultiPartition())) {
            return StorageStoreUtils.toDF(dataSegment, dataLayout.getLayout(), ss);
        }
        Dataset<Row> mergedDS = null;
        for (LayoutPartition partition : dataLayout.getMultiPartition()) {
            Dataset<Row> partitionDS = StorageStoreUtils.toDF(dataSegment, // 
                    dataLayout.getLayout(), //
                    partition.getPartitionId(), ss);
            if (Objects.isNull(mergedDS)) {
                mergedDS = partitionDS;
            } else {
                mergedDS = mergedDS.union(partitionDS);
            }
        }
        return mergedDS;
    }

}
