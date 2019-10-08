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

package io.kyligence.kap.metadata.cube.garbage;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.utils.IndexPlanReduceUtil;

public class RedundantLayoutGcStrategy implements IGarbageCleanerStrategy {

    @Override
    public Set<Long> collectGarbageLayouts(NDataflow dataflow) {
        Set<Long> garbageLayouts = Sets.newHashSet();
        IndexPlan indexPlan = dataflow.getIndexPlan();
        // reduce redundant layouts and indexes
        List<IndexEntity> indexes = indexPlan.getAllIndexes();
        List<IndexEntity> aggIndexList = indexes.stream() //
                .filter(indexEntity -> !indexEntity.isTableIndex()) //
                .collect(Collectors.toList());
        List<IndexEntity> tableIndexList = indexes.stream() //
                .filter(IndexEntity::isTableIndex) //
                .collect(Collectors.toList());
        Map<LayoutEntity, LayoutEntity> redundantToReservedMap = Maps.newHashMap();
        redundantToReservedMap.putAll(IndexPlanReduceUtil.collectRedundantLayoutsOfAggIndex(aggIndexList, true));
        redundantToReservedMap.putAll(IndexPlanReduceUtil.collectRedundantLayoutsOfTableIndex(tableIndexList, true));
        redundantToReservedMap.forEach((redundant, reserved) -> garbageLayouts.add(redundant.getId()));
        return garbageLayouts;
    }
}
