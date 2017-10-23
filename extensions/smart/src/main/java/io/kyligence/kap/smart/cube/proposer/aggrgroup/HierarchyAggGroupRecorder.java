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

package io.kyligence.kap.smart.cube.proposer.aggrgroup;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class HierarchyAggGroupRecorder {
    private Map<String, Node> nodeMap = Maps.newHashMap();

    private Node getNode(String dim) {
        Node ret = null;
        if (!nodeMap.containsKey(dim)) {
            nodeMap.put(dim, new Node(dim));
        }
        ret = nodeMap.get(dim);
        return ret;
    }

    public void add(String parent, String child) {
        Node parentNode = getNode(parent);
        parentNode.addNext(getNode(child));
    }

    private void remove(String col) {
        Node node = nodeMap.get(col);
        if (node != null) {
            for (Node n : node.children) {
                n.parent.addAll(node.parent);
            }
            for (Node n : node.parent) {
                n.children.remove(node);
            }
            node.parent.addAll(node.children);
            nodeMap.remove(col);
        }
    }

    private List<String> findLongestPath(Collection<Node> nodes) {
        // find heads
        Set<Node> nodeHeads = Sets.newHashSet();
        for (Node node : nodes) {
            if (node.parent.isEmpty()) {
                nodeHeads.add(node);
            }
        }

        if (nodeHeads.isEmpty()) {
            return null;
        }

        // propose result candidates
        List<List<String>> candidates = Lists.newArrayList();
        List<String> path = Lists.newArrayList();
        for (Node head : nodeHeads) {
            head.buildAllPath(path, candidates);
        }

        // order by elements number desc
        if (candidates.isEmpty()) {
            return null;
        } else {
            return Collections.max(candidates, new Comparator<List<String>>() {
                @Override
                public int compare(List<String> o1, List<String> o2) {
                    return o1.size() - o2.size();
                }
            });
        }
    }

    public List<List<String>> getResult(List<List<String>>... ignored) {
        if (ignored != null) {
            for (List<List<String>> l1 : ignored) {
                for (List<String> l2 : l1) {
                    for (String l3 : l2) {
                        remove(l3);
                    }
                }
            }
        }

        List<List<String>> result = Lists.newArrayList();
        while (!nodeMap.isEmpty()) {
            List<String> longestPath = findLongestPath(nodeMap.values());
            if (longestPath == null)
                break;

            result.add(longestPath);
            for (String pathNode : longestPath) {
                Node node = nodeMap.get(pathNode);
                for (Node parent : node.parent) {
                    parent.children.remove(node);
                }
                for (Node child : node.children) {
                    child.parent.remove(node);
                }
                nodeMap.remove(pathNode);
            }
        }
        return result;
    }

    private class Node {
        List<Node> parent = Lists.newArrayList();
        List<Node> children = Lists.newArrayList();
        String val;

        private Node(String val) {
            this.val = val;
        }

        void addNext(Node next) {
            next.parent.add(this);
            children.add(next);
        }

        void buildAllPath(List<String> path, List<List<String>> result) {
            path.add(val);
            if (children.isEmpty() && path.size() > 1) {
                result.add(Lists.newArrayList(path));
            }

            for (Node child : children) {
                child.buildAllPath(path, result);
            }
            path.remove(path.size() - 1);
        }
    }
}
