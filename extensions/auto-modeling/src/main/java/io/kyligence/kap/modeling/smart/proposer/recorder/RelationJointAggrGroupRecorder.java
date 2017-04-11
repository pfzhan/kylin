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

package io.kyligence.kap.modeling.smart.proposer.recorder;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.modeling.smart.util.Constants;

public class RelationJointAggrGroupRecorder {
    Map<String, Node> nodeMap = Maps.newHashMap();

    private Node getNode(String dim, double cost) {
        Node ret = null;
        if (!nodeMap.containsKey(dim)) {
            nodeMap.put(dim, new Node(dim, cost));
        }
        ret = nodeMap.get(dim);
        return ret;
    }

    public void add(String col1, double cost1, String col2, double cost2) {
        Node node1 = getNode(col1, cost1);
        Node node2 = getNode(col2, cost2);
        node1.neighbor.add(node2);
        node2.neighbor.add(node1);
    }

    public List<List<String>> getResult() {
        Collection<Node> nodes = nodeMap.values();
        Iterator<Node> itr = nodes.iterator();
        Set<SortedSet<Node>> candidates = Sets.newHashSet();
        while (itr.hasNext()) {
            Node n = itr.next();
            SortedSet<Node> path = Sets.newTreeSet();
            n.buildAllPath(path, candidates);

            for (Node nn : n.neighbor) {
                nn.neighbor.remove(n);
            }
            n.neighbor.clear();
        }

        List<List<String>> results = Lists.newArrayList();
        final Set<Node> usedCols = Sets.newHashSet();
        while (!candidates.isEmpty()) {
            SortedSet<Node> bestCandidate = Collections.max(candidates, new Comparator<SortedSet<Node>>() {
                @Override
                public int compare(SortedSet<Node> o1, SortedSet<Node> o2) {
                    return o1.size() - o2.size();
                }
            });

            List<String> newResult = Lists.newArrayList(Collections2.transform(Collections2.filter(bestCandidate, new Predicate<Node>() {
                @Override
                public boolean apply(@Nullable Node input) {
                    return !usedCols.contains(input);
                }
            }), new Function<Node, String>() {
                @Nullable
                @Override
                public String apply(@Nullable Node input) {
                    return input.value;
                }
            }));

            if (newResult.size() > 1) {
                results.add(newResult);
            }

            usedCols.addAll(bestCandidate);
            candidates.remove(bestCandidate);
        }

        return results;
    }

    private class Node implements Comparable<Node> {
        String value;
        double cost;
        SortedSet<Node> neighbor = Sets.newTreeSet();

        private Node(String value, double cost) {
            this.value = value;
            this.cost = cost;
        }

        void buildAllPath(final SortedSet<Node> path, Set<SortedSet<Node>> result) {
            if (!neighbor.containsAll(path)) {
                return;
            }

            path.add(this);
            Collection<Node> children = Collections2.filter(neighbor, new Predicate<Node>() {
                @Override
                public boolean apply(@Nullable Node input) {
                    return !path.contains(input);
                }
            });

            if (children.isEmpty() && path.size() > 1 && path.size() <= Constants.DIM_AGG_GROUP_JOINT_ELEMENTS_MAX) {
                result.add(Sets.newTreeSet(path));
            }

            for (Node child : children) {
                child.buildAllPath(path, result);
            }
            path.remove(this);
        }

        @Override
        public int compareTo(Node o) {
            int comp = this.value.compareTo(o.value);
            return comp;
        }
    }
}
