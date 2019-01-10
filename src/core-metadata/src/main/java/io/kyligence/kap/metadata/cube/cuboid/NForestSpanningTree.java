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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.Getter;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NForestSpanningTree extends NSpanningTree implements IKeepNames {
    @JsonProperty("nodes")
    private final Map<Long, TreeNode> nodesMap = Maps.newHashMap();
    private final Map<Long, LayoutEntity> layoutMap = Maps.newHashMap();

    /* If base cuboid exists, forest will become tree. */
    @JsonProperty("roots")
    private final List<TreeNode> roots = Lists.newArrayList();
    private int treeLevels;

    private static final Function<TreeNode, IndexEntity> TRANSFORM_FUNC = new Function<TreeNode, IndexEntity>() {
        @Nullable
        @Override
        public IndexEntity apply(@Nullable TreeNode input) {
            return input == null ? null : input.indexEntity;
        }
    };

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
    public boolean isValid(int requestCuboid) {
        return nodesMap.containsKey(requestCuboid);
    }

    @Override
    public int getCuboidCount() {
        return nodesMap.size();
    }

    @Override
    public Collection<IndexEntity> getRootIndexEntities() {
        return Collections2.transform(roots, TRANSFORM_FUNC);
    }

    @Override
    public Set<Integer> retrieveAllMeasures(IndexEntity root) {
        Set<Integer> measures = new LinkedHashSet<>();
        collectMeasures(measures, root);
        return measures;
    }

    @Override
    public Collection<LayoutEntity> getLayouts(IndexEntity indexEntity) {
        return (Collection<LayoutEntity>) cuboids.get(indexEntity);
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
    public IndexEntity getParentIndexEntity(IndexEntity cuboid) {
        if (nodesMap.get(cuboid.getId()) == null)
            return null;

        if (nodesMap.get(cuboid.getId()).parent == null)
            return null;

        return nodesMap.get(cuboid.getId()).parent.indexEntity;
    }

    @Override
    public IndexEntity getRootIndexEntity(IndexEntity cuboidDesc) {
        IndexEntity parent = cuboidDesc;
        while (getParentIndexEntity(parent) != null)
            parent = getParentIndexEntity(parent);
        return parent;
    }

    @Override
    public Collection<IndexEntity> getSpanningIndexEntities(IndexEntity cuboid) {
        return Collections2.transform(nodesMap.get(cuboid.getId()).children, TRANSFORM_FUNC);
    }

    @Override
    public Collection<IndexEntity> getAllIndexEntities() {
        return Collections2.transform(nodesMap.values(), TRANSFORM_FUNC);
    }

    @Override
    public void acceptVisitor(ISpanningTreeVisitor matcher) {
        Queue<TreeNode> queue = Lists.newLinkedList(roots);
        while (!queue.isEmpty()) {
            TreeNode head = queue.poll();
            boolean shouldContinue = matcher.visit(head.indexEntity);
            if (shouldContinue)
                queue.addAll(head.children);
        }
    }

    private void collectMeasures(Set<Integer> measures, IndexEntity parent) {
        measures.addAll(parent.getEffectiveMeasures().keySet());
        for (IndexEntity cuboid : getSpanningIndexEntities(parent)) {
            collectMeasures(measures, cuboid);
        }
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
        private TreeNode parent;
        @JsonProperty("level")
        private int level;

        private void addChild(TreeNode child) {
            children.add(child);
        }

        private TreeNode(IndexEntity indexEntity) {
            this.indexEntity = indexEntity;
        }
    }

    private class TreeBuilder {
        // Sort in descending order of dimension number to make sure parent is in front
        // of children.
        private SortedSet<IndexEntity> cuboids = Sets.newTreeSet(new Comparator<IndexEntity>() {
            @Override
            public int compare(IndexEntity o1, IndexEntity o2) {
                int c = Integer.compare(o2.getDimensions().size(), o1.getDimensions().size());
                if (c != 0)
                    return c;
                else
                    return Long.compare(o1.getId(), o2.getId());
            }
        });

        private TreeBuilder(Collection<IndexEntity> cuboids) {
            if (cuboids != null)
                this.cuboids.addAll(cuboids);
        }

        private void build() {
            for (IndexEntity cuboid : cuboids) {
                addCuboid(cuboid);
            }
        }

        private TreeNode findBestParent(IndexEntity cuboid) {
            TreeNode parent = null;
            for (TreeNode root : roots) {
                parent = doFindBestParent(cuboid, root);
                if (parent != null)
                    break;
            }
            return parent;
        }

        protected TreeNode doFindBestParent(IndexEntity cuboid, TreeNode parent) {
            if (!parent.indexEntity.dimensionDerive(cuboid)) {
                return null;
            }

            if (!cuboid.bothTableIndexOrNot(parent.indexEntity))
                return null;

            List<TreeNode> candidates = Lists.newArrayList();
            for (TreeNode child : parent.children) {
                TreeNode candidate = doFindBestParent(cuboid, child);
                if (candidate != null) {
                    candidates.add(candidate);
                }
            }
            if (candidates.isEmpty()) {
                candidates.add(parent);
            }

            return Collections.min(candidates, new Comparator<TreeNode>() {
                @Override
                public int compare(TreeNode o1, TreeNode o2) {
                    return o1.indexEntity.getDimensions().size() - o2.indexEntity.getDimensions().size(); // TODO: compare
                    // with row size
                }
            });
        }

        private void addCuboid(IndexEntity cuboid) {
            TreeNode node = new TreeNode(cuboid);
            TreeNode parent = findBestParent(cuboid);
            if (parent != null) {
                parent.addChild(node);
                node.parent = parent;
                node.level = parent.level + 1;
                treeLevels = Math.max(treeLevels, node.level);
            } else {
                node.level = 0;
                roots.add(node);
            }

            nodesMap.put(cuboid.getId(), node);
            for (LayoutEntity layout : cuboid.getLayouts()) {
                layoutMap.put(layout.getId(), layout);
            }
        }
    }

}