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
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.Segments;

@SuppressWarnings("serial")
/*
* Dirty fix web display spanning tree by use the old way that generate the spanning tree.
* Only used in web display spanning tree.
* */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NSpanningTreeForWeb extends NSpanningTree implements IKeepNames {
    @JsonProperty("nodes")
    private Map<Long, TreeNodeForWeb> nodesMap = Maps.newTreeMap();

    /* If base cuboid exists, forest will become tree. */
    @JsonProperty("roots")
    private final List<TreeNodeForWeb> roots = Lists.newArrayList();

    @JsonIgnore
    private IndexPlan indexPlan;

    private static final Function<TreeNode, IndexEntity> TRANSFORM_FUNC = new Function<TreeNode, IndexEntity>() {
        @Nullable
        @Override
        public IndexEntity apply(@Nullable TreeNode input) {
            return input == null ? null : input.indexEntity;
        }
    };

    public NSpanningTreeForWeb(Map<IndexEntity, Collection<LayoutEntity>> cuboids, IndexPlan indexPlan) {
        super(cuboids, indexPlan.getUuid());
        this.indexPlan = indexPlan;
        init();
    }

    public Map<Long, TreeNodeForWeb> getNodesMap() {
        return nodesMap;
    }

    public List<TreeNodeForWeb> getRoots() {
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

        // map <root cuboid's id, map <dimension size, list of nodes having the same dimension size>>
        // this map is convenient when finding the best parent for a cuboid from bottom up
        private Map<Long, TreeMap<Integer, List<TreeNodeForWeb>>> dimensionSizeMap = Maps.newHashMap();

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
            TreeNodeForWeb node = new TreeNodeForWeb(cuboid);
            node.setCuboid(getSimplifiedCuboidResponse(cuboid));
            TreeNodeForWeb parent = findBestParent(cuboid);
            if (parent != null) {
                parent.children.add(node);
                parent.childrenIds.add(cuboid.getId());
                node.parent = parent;
                node.parentId = parent.getIndexEntity().getId();
                node.level = parent.level + 1;
                node.rootIndexId = parent.rootIndexId;
            } else {
                node.level = 0;
                node.rootIndexId = cuboid.getId();
                roots.add(node);
                dimensionSizeMap.put(node.getIndexEntity().getId(), new TreeMap<>());
            }

            Map<Integer, List<TreeNodeForWeb>> map = dimensionSizeMap.get(node.rootIndexId);
            List<TreeNodeForWeb> nodes = map.get(cuboid.getDimensions().size());

            if (nodes == null)
                map.put(cuboid.getDimensions().size(), Lists.newArrayList(node));
            else
                nodes.add(node);

            nodesMap.put(cuboid.getId(), node);
        }

        private TreeNodeForWeb findBestParent(IndexEntity cuboid) {
            for (TreeNode root : roots) {
                if (!root.getIndexEntity().fullyDerive(cuboid))
                    continue;

                for (int dimensionSize : dimensionSizeMap.get(root.indexEntity.getId()).keySet()) {
                    if (dimensionSize <= cuboid.getDimensions().size())
                        continue;

                    for (TreeNodeForWeb node : dimensionSizeMap.get(root.indexEntity.getId()).get(dimensionSize)) {
                        if (node.indexEntity.fullyDerive(cuboid)) {
                            return node;
                        }
                    }
                }
            }

            return null;
        }

        private SimplifiedCuboidResponse getSimplifiedCuboidResponse(IndexEntity cuboid) {
            val dataflow = NDataflowManager.getInstance(indexPlan.getConfig(), indexPlan.getProject())
                    .getDataflow(indexPlan.getUuid());
            Segments<NDataSegment> segments = dataflow.getSegments().getSegmentsExcludeRefreshingAndMerging();

            if (CollectionUtils.isEmpty(segments)) {
                return new SimplifiedCuboidResponse(cuboid.getId(), CuboidStatus.EMPTY, 0L);
            }

            long storage = 0L;
            for (NDataSegment segment : segments) {
                for (LayoutEntity layout : cuboid.getLayouts()) {
                    NDataLayout dataLayout = segment.getLayout(layout.getId());
                    if (dataLayout == null) {
                        return new SimplifiedCuboidResponse(cuboid.getId(), CuboidStatus.EMPTY, 0L);
                    }
                    storage += dataLayout.getByteSize();
                }
            }

            return new SimplifiedCuboidResponse(cuboid.getId(), CuboidStatus.AVAILABLE, storage);
        }
    }

    @Getter
    @Setter
    public static class TreeNodeForWeb extends TreeNode {

        @JsonProperty("cuboid")
        private SimplifiedCuboidResponse cuboid;
        @JsonProperty("children")
        private List<Long> childrenIds = Lists.newLinkedList();
        @JsonProperty("parent")
        private long parentId = -1;

        @JsonIgnore
        private long rootIndexId;

        public TreeNodeForWeb(IndexEntity indexEntity) {
            super(indexEntity);
            cuboid = new SimplifiedCuboidResponse();
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SimplifiedCuboidResponse implements Serializable {
        @JsonProperty("id")
        private long id;
        @JsonProperty("status")
        private CuboidStatus status = CuboidStatus.AVAILABLE;
        @JsonProperty("storage_size")
        private long storageSize;
    }
}