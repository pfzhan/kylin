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

package io.kyligence.kap.rest.response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.cuboid.NForestSpanningTree;
import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Setter
@Getter
public class NSpanningTreeResponse {

    @JsonProperty("nodes")
    private final Map<Long, TreeNodeResponse> nodesMap = Maps.newHashMap();

    @JsonProperty("roots")
    private final List<TreeNodeResponse> roots = Lists.newArrayList();

    public NSpanningTreeResponse() {

    }
    public NSpanningTreeResponse(NForestSpanningTree tree, NDataModel model) {
        val cubePlan = NCubePlanManager.getInstance(model.getConfig(), model.getProject())
                .findMatchingCubePlan(model.getName());
        for (NForestSpanningTree.TreeNode root : tree.getRoots()) {
            roots.add(simplifyTreeNodeResponse(root, cubePlan));
        }
        for (HashMap.Entry<Long, NForestSpanningTree.TreeNode> entry : tree.getNodesMap().entrySet()) {
            nodesMap.put(entry.getKey(), simplifyTreeNodeResponse(entry.getValue(), cubePlan));
        }
    }

    private NSpanningTreeResponse.TreeNodeResponse simplifyTreeNodeResponse(NForestSpanningTree.TreeNode root,
            NCubePlan cubePlan) {
        NSpanningTreeResponse.TreeNodeResponse treeNodeResponse = new NSpanningTreeResponse.TreeNodeResponse();
        treeNodeResponse.setLevel(root.getLevel());
        treeNodeResponse.setCuboid(simplifyCuboidResponse(root.getCuboidDesc().getId(), cubePlan));
        if (root.getParent() != null) {
            treeNodeResponse.setParent(root.getParent().getCuboidDesc().getId());
        }
        List<Long> childrenIds = Lists.newArrayList();
        for (NForestSpanningTree.TreeNode children : root.getChildren()) {
            childrenIds.add(children.getCuboidDesc().getId());
        }
        treeNodeResponse.setChildren(childrenIds);
        return treeNodeResponse;
    }

    private NSpanningTreeResponse.SimplifiedCuboidResponse simplifyCuboidResponse(long id, NCubePlan cubePlan) {
        val cuboidDesc = cubePlan.getCuboidDesc(id);
        CuboidDescResponse cuboidDescResponse = new CuboidDescResponse(cuboidDesc);
        NSpanningTreeResponse.SimplifiedCuboidResponse simplifiedCuboidResponse = new NSpanningTreeResponse.SimplifiedCuboidResponse();
        simplifiedCuboidResponse.setId(id);
        simplifiedCuboidResponse.setStatus(cuboidDescResponse.getStatus());
        simplifiedCuboidResponse.setStorageSize(cuboidDescResponse.getStorageSize());
        return simplifiedCuboidResponse;
    }

    @Getter
    @Setter
    public static class TreeNodeResponse {

        @JsonProperty("cuboid")
        private SimplifiedCuboidResponse cuboid;
        @JsonProperty("children")
        private List<Long> children = Lists.newLinkedList();
        @JsonProperty("parent")
        private long parent = -1;
        @JsonProperty("level")
        private int level;
    }

    @Getter
    @Setter
    public static class SimplifiedCuboidResponse {
        @JsonProperty("id")
        private long id;
        @JsonProperty("status")
        private CuboidStatus status = CuboidStatus.AVAILABLE;
        @JsonProperty("storage_size")
        private long storageSize;
    }

}
