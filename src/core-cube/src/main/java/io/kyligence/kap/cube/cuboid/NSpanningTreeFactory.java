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

package io.kyligence.kap.cube.cuboid;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import io.kyligence.kap.cube.model.LayoutEntity;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ClassUtil;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.cube.model.IndexPlan;
import io.kyligence.kap.cube.model.IndexEntity;

public class NSpanningTreeFactory {
    public static NSpanningTree fromIndexPlan(IndexPlan indexPlan) {
        Map<IndexEntity, Collection<LayoutEntity>> descLayouts = Maps.newHashMap();
        for (IndexEntity indexEntity : indexPlan.getAllIndexes()) {
            descLayouts.put(indexEntity, indexEntity.getLayouts());
        }
        return newInstance(KapConfig.wrap(indexPlan.getConfig()), descLayouts, indexPlan.getUuid());
    }

    public static NSpanningTree fromIndexes(Map<IndexEntity, Collection<LayoutEntity>> cuboids, String cacheKey) {
        return newInstance(KapConfig.getInstanceFromEnv(), cuboids, cacheKey);
    }

    public static NSpanningTree fromLayouts(Collection<LayoutEntity> layoutEntities, String cacheKey) {
        Map<IndexEntity, Collection<LayoutEntity>> descLayouts = Maps.newHashMap();
        for (LayoutEntity layout : layoutEntities) {
            IndexEntity cuboidDesc = layout.getIndex();
            if (descLayouts.get(cuboidDesc) == null) {
                Set<LayoutEntity> layouts = Sets.newHashSet();
                layouts.add(layout);
                descLayouts.put(cuboidDesc, layouts);
            } else {
                descLayouts.get(cuboidDesc).add(layout);
            }
        }
        return fromIndexes(descLayouts, cacheKey);
    }

    private static NSpanningTree newInstance(KapConfig kapConfig, Map<IndexEntity, Collection<LayoutEntity>> cuboids,
            String cacheKey) {
        try {
            String clzName = kapConfig.getCuboidSpanningTree();
            Class<? extends NSpanningTree> clz = ClassUtil.forName(clzName, NSpanningTree.class);
            return clz.getConstructor(Map.class, String.class).newInstance(cuboids, cacheKey);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
