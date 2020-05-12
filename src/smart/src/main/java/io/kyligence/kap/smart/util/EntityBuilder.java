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

package io.kyligence.kap.smart.util;

import java.util.Collection;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.Setter;

public class EntityBuilder {
    public static class LayoutEntityBuilder {
        private long layoutId;
        private List<Integer> colOrderIds = Lists.newArrayList();
        private List<Integer> partitionCols = Lists.newArrayList();
        private List<Integer> shardByCols = Lists.newArrayList();
        private IndexEntity index;
        private boolean isAuto;

        public LayoutEntityBuilder(long layoutId, IndexEntity index) {
            this.layoutId = layoutId;
            this.index = index;
        }

        public LayoutEntityBuilder colOrderIds(List<Integer> colOrderIds) {
            this.colOrderIds = colOrderIds;
            return this;
        }

        public LayoutEntityBuilder partitionCol(List<Integer> partitionCols) {
            this.partitionCols = partitionCols;
            return this;
        }

        public LayoutEntityBuilder shardByCol(List<Integer> shardByCols) {
            this.shardByCols = shardByCols;
            return this;
        }

        public LayoutEntityBuilder isAuto(boolean isAuto) {
            this.isAuto = isAuto;
            return this;
        }

        public LayoutEntity build() {
            LayoutEntity layout = new LayoutEntity();
            layout.setColOrder(colOrderIds);
            layout.setId(layoutId);
            layout.setIndex(index);
            layout.setAuto(isAuto);
            layout.setUpdateTime(System.currentTimeMillis());
            layout.setPartitionByColumns(partitionCols);
            layout.setShardByColumns(shardByCols);
            return layout;
        }

    }

    public static class IndexEntityBuilder {

        public static long findAvailableIndexEntityId(IndexPlan indexPlan, Collection<IndexEntity> existedIndex,
                boolean isTableIndex) {
            long result = isTableIndex ? IndexEntity.TABLE_INDEX_START_ID : 0;
            List<Long> cuboidIds = Lists.newArrayList();
            for (IndexEntity indexEntity : existedIndex) {
                long indexEntityId = indexEntity.getId();
                if ((isTableIndex && IndexEntity.isTableIndex(indexEntityId))
                        || (!isTableIndex && indexEntityId < IndexEntity.TABLE_INDEX_START_ID)) {
                    cuboidIds.add(indexEntityId);
                }
            }

            if (!cuboidIds.isEmpty()) {
                // use the largest cuboid id + step
                cuboidIds.sort(Long::compareTo);
                result = cuboidIds.get(cuboidIds.size() - 1) + IndexEntity.INDEX_ID_STEP;
            }
            return Math.max(result,
                    isTableIndex ? indexPlan.getNextTableIndexId() : indexPlan.getNextAggregationIndexId());
        }

        private long id;
        private IndexPlan indexPlan;
        @Setter
        private List<Integer> dimIds = Lists.newArrayList();
        @Setter
        private List<Integer> measureIds = Lists.newArrayList();
        private List<LayoutEntity> layouts = Lists.newArrayList();

        public IndexEntityBuilder(long id, IndexPlan indexPlan) {
            this.id = id;
            this.indexPlan = indexPlan;
        }

        public IndexEntityBuilder dimIds(List<Integer> dimIds) {
            this.dimIds = dimIds;
            return this;
        }

        public IndexEntityBuilder measure(List<Integer> measureIds) {
            this.measureIds = measureIds;
            return this;
        }

        public IndexEntityBuilder addLayout(LayoutEntity layout) {
            this.layouts.add(layout);
            return this;
        }

        public IndexEntityBuilder addLayouts(List<LayoutEntity> layouts) {
            this.layouts.addAll(layouts);
            return this;
        }

        public IndexEntityBuilder setLayout(List<LayoutEntity> layouts) {
            this.layouts.clear();
            this.layouts.addAll(layouts);
            return this;
        }

        public IndexEntity build() {
            Preconditions.checkState(!dimIds.isEmpty() || !measureIds.isEmpty(),
                    "Neither dimension nor measure could be proposed for indexEntity");

            IndexEntity indexEntity = new IndexEntity();
            indexEntity.setId(id);
            indexEntity.setDimensions(dimIds);
            indexEntity.setMeasures(Lists.newArrayList(measureIds));
            indexEntity.setIndexPlan(indexPlan);
            indexEntity.setLayouts(layouts);
            return indexEntity;
        }
    }

}
