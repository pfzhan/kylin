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

package io.kyligence.kap.metadata.cube.cuboid;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.Getter;

public abstract class NSpanningTree implements Serializable {
    final protected Map<IndexEntity, Collection<LayoutEntity>> cuboids;
    final protected String cacheKey;

    public NSpanningTree(Map<IndexEntity, Collection<LayoutEntity>> cuboids, String cacheKey) {
        long totalSize = 0L;
        for (Collection<LayoutEntity> entities : cuboids.values()) {
            totalSize += entities.size();
        }
        long maxCombination = KylinConfig.getInstanceFromEnv().getCubeAggrGroupMaxCombination() * 10;
        Preconditions.checkState(totalSize <= maxCombination,
                "Too many cuboids for the cube. Cuboid combination reached " + totalSize + " and limit is "
                        + maxCombination + ". Abort calculation.");
        this.cuboids = cuboids;
        this.cacheKey = cacheKey;
    }

    abstract public boolean isValid(long cuboidId);

    abstract public int getCuboidCount();

    abstract public Collection<IndexEntity> getRootIndexEntities();

    abstract public Collection<LayoutEntity> getLayouts(IndexEntity cuboidDesc);

    abstract public IndexEntity getIndexEntity(long cuboidId);

    abstract public LayoutEntity getCuboidLayout(long cuboidLayoutId);

    abstract public void decideTheNextLayer(Collection<IndexEntity> currentLayer, NDataSegment segment);

    abstract public Collection<IndexEntity> getChildrenByIndexPlan(IndexEntity parent);

    abstract public Collection<IndexEntity> getAllIndexEntities();

    public Map<IndexEntity, Collection<LayoutEntity>> getCuboids() {
        return cuboids;
    }

    @Getter
    public static class TreeNode implements Serializable {
        @JsonProperty("cuboid")
        protected final IndexEntity indexEntity;

        @JsonProperty("children")
        protected final ArrayList<TreeNode> children = Lists.newArrayList();

        @JsonProperty("level")
        protected int level;

        protected transient TreeNode parent;
        protected transient List<IndexEntity> parentCandidates;
        protected transient boolean hasBeenDecided = false;

        public TreeNode(IndexEntity indexEntity) {
            this.indexEntity = indexEntity;
        }

        @Override
        public String toString() {
            return "level:" + level + ", node:" + indexEntity.getId() + //
                    ", dim:" + indexEntity.getDimensionBitset().toString() + //
                    ", measure:" + indexEntity.getMeasureBitset().toString() + //
                    ", children:{" + children.toString() + "}";//
        }
    }
}
