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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.Getter;
import lombok.val;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NForestSpanningTree extends NSpanningTree implements IKeepNames {
    @JsonProperty("nodes")
    private Map<Long, TreeNode> nodesMap = Maps.newTreeMap();
    private final Map<Long, LayoutEntity> layoutMap = Maps.newHashMap();

    /* If base cuboid exists, forest will become tree. */
    @JsonProperty("roots")
    private final List<TreeNode> roots = Lists.newArrayList();

    private static final Function<TreeNode, IndexEntity> TRANSFORM_FUNC = new Function<TreeNode, IndexEntity>() {
        @Nullable
        @Override
        public IndexEntity apply(@Nullable TreeNode input) {
            return input == null ? null : input.indexEntity;
        }
    };

    private static final Logger logger = LoggerFactory.getLogger(NForestSpanningTree.class);

    public NForestSpanningTree(Map<IndexEntity, Collection<LayoutEntity>> cuboids, String cacheKey) {
        super(cuboids, cacheKey);
        init();
    }

    public Map<Long, TreeNode> getNodesMap() {
        return nodesMap;
    }

    public List<TreeNode> getRoots() {
        return roots;
    }

    @Override
    public boolean isValid(long requestCuboid) {
        return nodesMap.containsKey(requestCuboid);
    }

    @Override
    public int getCuboidCount() {
        return nodesMap.size();
    }

    @Override
    public Collection<IndexEntity> getRootIndexEntities() {
        return Collections2.transform(roots, TRANSFORM_FUNC::apply);
    }

    @Override
    public Collection<LayoutEntity> getLayouts(IndexEntity indexEntity) {
        return cuboids.get(indexEntity);
    }

    @Override
    public IndexEntity getIndexEntity(long cuboidId) {
        if (nodesMap.get(cuboidId) == null) {
            throw new IllegalStateException("Cuboid（ID:" + cuboidId + ") does not exist!");
        }
        return nodesMap.get(cuboidId).indexEntity;
    }

    @Override
    public LayoutEntity getCuboidLayout(long cuboidLayoutId) {
        return layoutMap.get(cuboidLayoutId);
    }

    @Override
    public void decideTheNextLayer(Collection<IndexEntity> currentLayer, NDataSegment segment) {
        // After built, we know each cuboid's size.
        // Then we will find each cuboid's children.
        // Smaller cuboid has smaller cost, and has higher priority when finding children.
        Comparator<IndexEntity> c1 = Comparator.comparingLong(o -> getRows(o, segment));

        // for deterministic
        Comparator<IndexEntity> c2 = Comparator.comparingLong(IndexEntity::getId);

        val orderedIndexes = currentLayer.stream() //
                .sorted(c1.thenComparing(c2)) //
                .collect(Collectors.toList()); //

        orderedIndexes.forEach(index -> {
            adjustTree(index, segment);

            logger.info("Adjust spanning tree." + //
            " Current index plan: {}." + //
            " Current index entity: {}." + //
            " Its children: {}\n" //
            , index.getIndexPlan().getUuid() //
            , index.getId() //
            , Arrays.toString(getChildrenByIndexPlan(index).stream() //
                    .map(IndexEntity::getId).toArray())//
            );
        });

    }

    @Override
    public Collection<IndexEntity> getChildrenByIndexPlan(IndexEntity parent) {
        // only meaningful when parent has been called in decideTheNextLayer
        TreeNode parentNode = nodesMap.get(parent.getId());
        Preconditions.checkState(parentNode.hasBeenDecided, "Node must have been decided before get its children.");
        return Collections2.transform(parentNode.children, TRANSFORM_FUNC::apply);
    }

    @Override
    public Collection<IndexEntity> getAllIndexEntities() {
        return Collections2.transform(nodesMap.values(), TRANSFORM_FUNC::apply);
    }

    private void adjustTree(IndexEntity parent, NDataSegment seg) {
        TreeNode parentNode = nodesMap.get(parent.getId());

        List<TreeNode> children = nodesMap.values().stream() //
                .filter(node -> shouldBeAdded(node, parent, seg))
                .collect(Collectors.toList());//

        // update child node's parent.
        children.forEach(node -> {
            node.level = parentNode.level + 1;
            node.parent = parentNode;
        });

        // update parent node's children.
        parentNode.children.addAll(children);
        parentNode.hasBeenDecided = true;
    }

    private boolean shouldBeAdded(TreeNode node, IndexEntity parent, NDataSegment seg) {
        return node.parent == null // already has been decided
                && node.parentCandidates != null //it is root node
                && node.parentCandidates.stream().allMatch(c -> isBuilt(c, seg)) // its parents candidates is not all ready.
                && node.parentCandidates.contains(parent); //its parents candidates did not contains this IndexEntity.
    }

    public boolean isBuilt(IndexEntity ie, NDataSegment seg) {
        return getLayoutFromSeg(ie, seg) != null;
    }

    private long getRows(IndexEntity ie, NDataSegment seg) {
        return getLayoutFromSeg(ie, seg).getRows();
    }

    private NDataLayout getLayoutFromSeg(IndexEntity ie, NDataSegment seg) {
        return seg.getLayout(Lists.newArrayList(getLayouts(ie)).get(0).getId());
    }

    private void init() {
        new TreeBuilder(cuboids.keySet()).build();
    }

    @Getter
    public class TreeNode implements Serializable {
        @JsonProperty("cuboid")
        private final IndexEntity indexEntity;

        @JsonProperty("children")
        private final List<TreeNode> children = Lists.newLinkedList();

        @JsonProperty("level")
        private int level;

        private TreeNode parent;
        private List<IndexEntity> parentCandidates;
        private boolean hasBeenDecided = false;

        private TreeNode(IndexEntity indexEntity) {
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

    private class TreeBuilder {
        // Sort in descending order of dimension and measure number to make sure children is in front
        // of parent.
        private SortedSet<IndexEntity> sortedCuboids = Sets.newTreeSet((o1, o2) -> {
            int c = Integer.compare(o1.getDimensions().size(), o2.getDimensions().size());
            if (c != 0)
                return c;
            else
                return Long.compare(o1.getId(), o2.getId());
        });

        private TreeBuilder(Collection<IndexEntity> cuboids) {
            if (cuboids != null)
                this.sortedCuboids.addAll(cuboids);
        }

        private void build() {
            for (IndexEntity cuboid : sortedCuboids) {
                addCuboid(cuboid);
            }
        }

        private void addCuboid(IndexEntity cuboid) {
            TreeNode node = new TreeNode(cuboid);
            List<IndexEntity> candidates = findDirectParentCandidates(cuboid);
            if (!candidates.isEmpty()) {
                node.parentCandidates = candidates;
            } else {
                node.level = 0;
                roots.add(node);
            }

            nodesMap.put(cuboid.getId(), node);
            for (LayoutEntity layout : cuboid.getLayouts()) {
                layoutMap.put(layout.getId(), layout);
            }
        }

        // decide every cuboid's direct parent candidates(eg ABCD->ABC->AB, ABCD is ABC's direct parent, but not AB's).
        // but will not decide the cuboid tree.
        // when in building, will find the best one as the cuboid's parent.
        private List<IndexEntity> findDirectParentCandidates(IndexEntity entity) {
            List<IndexEntity> candidates = new ArrayList<>();
            for (IndexEntity cuboid : sortedCuboids) {

                if (!cuboid.fullyDerive(entity)) {
                    continue;
                }

                // only add direct parent
                if (candidates.stream().noneMatch(candidate -> cuboid.fullyDerive(candidate))) {
                    candidates.add(cuboid);
                }
            }

            return candidates;
        }
    }
}