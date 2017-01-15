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

package io.kyligence.kap.modeling.auto.tuner.model;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class HierarchyAggGroupRecorder extends AbstractAggGroupRecorder {
    private Map<String, Node> nodeMap = Maps.newHashMap();

    class Node {
        Node parent;
        List<Node> children = Lists.newArrayList();
        String val;

        Node(String val) {
            this.val = val;
        }

        void addNext(Node next) {
            children.add(next);
        }

        void buildAllPath(List<String> path, List<List<String>> result) {
            path.add(val);
            if (children.isEmpty()) {
                result.add(Lists.newArrayList(path));
            }

            for (Node child : children) {
                child.buildAllPath(path, result);
            }
            path.remove(path.size() - 1);
        }
    }

    private Node getNode(String dim) {
        Node ret = null;
        if (!nodeMap.containsKey(dim)) {
            nodeMap.put(dim, new Node(dim));
        }
        ret = nodeMap.get(dim);
        return ret;
    }

    public List<List<String>> getAggGroups() {
        // build node graph
        for (Map.Entry<List<String>, AggGroupRecord> entry : aggGroupRecords.entrySet()) {
            Node prev = getNode(entry.getKey().get(0));
            Node curr = null;
            for (int i = 1; i < entry.getKey().size(); i++) {
                curr = getNode(entry.getKey().get(i));
                prev.addNext(curr);
                curr.parent = prev;
                prev = curr;
            }
        }

        // find heads
        Set<Node> nodeHeads = Sets.newHashSet();
        for (Node node : nodeMap.values()) {
            if (node.parent == null) {
                nodeHeads.add(node);
            }
        }

        // build result candidates
        List<List<String>> candidates = Lists.newArrayList();
        List<String> path = Lists.newArrayList();
        for (Node head : nodeHeads) {
            head.buildAllPath(path, candidates);
        }

        // order by elements number
        Collections.sort(candidates, Collections.reverseOrder(new Comparator<List<String>>() {
            @Override
            public int compare(List<String> o1, List<String> o2) {
                return o1.size() - o2.size();
            }
        }));

        // remove duplicated
        List<List<String>> results = Lists.newArrayList();
        outer: for (List<String> candidate : candidates) {
            for (List<String> result : results) {
                Collection<String> intersect = CollectionUtils.intersection(result, candidate);
                if (!intersect.isEmpty()) {
                    continue outer;
                }
            }
            results.add(candidate);
        }

        return results;
    }
}
