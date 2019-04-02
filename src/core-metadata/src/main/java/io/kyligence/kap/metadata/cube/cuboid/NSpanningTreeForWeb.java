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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;

@SuppressWarnings("serial")
/*
* Dirty fix web display spanning tree by use the old way that generate the spanning tree.
* Only used in web display spanning tree.
* */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NSpanningTreeForWeb extends NSpanningTree implements IKeepNames {
    @JsonProperty("nodes")
    private Map<Long, TreeNode> nodesMap = Maps.newTreeMap();

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

    NSpanningTreeForWeb(Map<IndexEntity, Collection<LayoutEntity>> cuboids, String cacheKey) {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public void decideTheNextLayer(Collection<IndexEntity> currentLayer, NDataSegment segment) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<IndexEntity> getChildrenByIndexPlan(IndexEntity parent) {
        return Collections2.transform(nodesMap.get(parent.getId()).children, TRANSFORM_FUNC::apply);
    }

    @Override
    public Collection<IndexEntity> getAllIndexEntities() {
        return Collections2.transform(nodesMap.values(), TRANSFORM_FUNC::apply);
    }

    private void init() {
        new TreeBuilder(cuboids.keySet()).build();
    }

    private class TreeBuilder {
        // Sort in descending order of dimension and measure number to make sure parent is in front
        // of children.
        private SortedSet<IndexEntity> sortedCuboids = Sets.newTreeSet((o1, o2) -> {
            int c = Integer.compare(o2.getDimensions().size(), o1.getDimensions().size());
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
            TreeNode parent = findBestParent(cuboid);
            if (parent != null) {
                parent.children.add(node);
                node.parent = parent;
                node.level = parent.level + 1;
            } else {
                node.level = 0;
                roots.add(node);
            }
            nodesMap.put(cuboid.getId(), node);
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

        private TreeNode doFindBestParent(IndexEntity cuboid, TreeNode parent) {
            if (!parent.indexEntity.fullyDerive(cuboid)) {
                return null;
            }

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

            return Collections.min(candidates, Comparator.comparingInt(o -> o.indexEntity.getDimensions().size()));
        }

    }
}