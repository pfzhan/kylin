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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.PartitionStatusEnum;
import io.kyligence.kap.metadata.cube.model.SegmentPartition;

public class PartitionDictionaryBuilderHelper extends DictionaryBuilderHelper {

    public static Set<TblColRef> extractTreeRelatedGlobalDictToBuild(NDataSegment seg,
            Collection<IndexEntity> toBuildIndexEntities) {
        List<LayoutEntity> toBuildCuboids = Lists.newArrayList();
        for (IndexEntity desc : toBuildIndexEntities) {
            toBuildCuboids.addAll(desc.getLayouts());
        }

        List<LayoutEntity> buildedLayouts = Lists.newArrayList();
        if (seg.getSegDetails() != null) {
            Set<SegmentPartition> newPartitions = seg.getMultiPartitions().stream()
                    .filter(partition -> !partition.getStatus().equals(PartitionStatusEnum.READY))
                    .collect(Collectors.toSet());
            if (CollectionUtils.isEmpty(newPartitions)) {
                for (NDataLayout cuboid : seg.getSegDetails().getLayouts()) {
                    buildedLayouts.add(cuboid.getLayout());
                }
            }
        }
        Set<TblColRef> buildedColRefSet = findNeedDictCols(buildedLayouts);
        Set<TblColRef> toBuildColRefSet = findNeedDictCols(toBuildCuboids);
        toBuildColRefSet.removeIf(buildedColRefSet::contains);
        return toBuildColRefSet;
    }
}
