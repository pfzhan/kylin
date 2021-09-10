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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.obf.IKeepNames;
import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.val;

@Deprecated
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NForestSpanningTreeV2 extends NForestSpanningTree implements IKeepNames {

    private static final Logger logger = LoggerFactory.getLogger(NForestSpanningTreeV2.class);

    public NForestSpanningTreeV2(Map<IndexEntity, Collection<LayoutEntity>> cuboids, String cacheKey) {
        super(cuboids, cacheKey);
    }

    private List<TreeNode> getAllLeafNodes() {
        val roots = getRoots();
        List<TreeNode> leafNodes = Lists.newArrayList();
        roots.forEach(root -> leafNodes.addAll(getTreeLeafNodes(root)));
        return leafNodes;
    }

    private List<TreeNode> getTreeLeafNodes(TreeNode node) {
        List<TreeNode> nodes = Lists.newArrayList();
        if (node.children.isEmpty()) {
            nodes.add(node);
        } else {
            node.children.forEach(child -> nodes.addAll(getTreeLeafNodes(child)));
        }
        return nodes;
    }

    @Override
    public Collection<IndexEntity> decideTheNextBatch(NDataSegment segment) {
        List<TreeNode> nextBatchIndex = Lists.newArrayList();
        // Smaller cuboid has smaller cost, and has higher priority when finding children.
        Comparator<IndexEntity> c1 = Comparator.comparingLong(o -> getRows(o, segment));

        // for deterministic
        Comparator<IndexEntity> c2 = Comparator.comparingLong(IndexEntity::getId);

        List<TreeNode> leafNodes = getAllLeafNodes();
        val orderedIndexes = leafNodes.stream().map(leaf -> leaf.indexEntity) //
                .filter(index -> isBuilt(index, segment)) //
                .sorted(c1.thenComparing(c2)) //
                .collect(Collectors.toList());

        orderedIndexes.forEach(index -> {
            nextBatchIndex.addAll(adjustTree(index, segment, false).children);

            logger.info("Adjust spanning tree." + //
            " Current index plan: {}." + //
            " Current index entity: {}." + //
            " Its children: {}\n", //
                    index.getIndexPlan().getUuid(), //
                    index.getId(), //
                    Arrays.toString(getChildrenByIndexPlan(index).stream() //
                            .map(IndexEntity::getId).toArray())//
            );
        });

        return Collections2.transform(nextBatchIndex, new Function<TreeNode, IndexEntity>() {
            @Override
            public IndexEntity apply(NSpanningTree.TreeNode node) {
                return node.indexEntity;
            }
        });
    }
}
