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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.smart.common.SmartConfig;

public class RelationJointAggrGroupRecorder {
    private final SmartConfig smartConfig;

    public RelationJointAggrGroupRecorder(SmartConfig smartConfig) {
        this.smartConfig = smartConfig;
    }

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

    private void remove(String col) {
        Node node = nodeMap.get(col);
        if (node != null) {
            for (Node n : node.neighbor) {
                n.neighbor.remove(node);
            }
            node.neighbor.clear();
            nodeMap.remove(col);
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

        Collection<Node> nodes = nodeMap.values();
        Set<SortedSet<Node>> candidates = Sets.newHashSet();

        for (Node node : nodes) {
            Set<SortedSet<Node>> tmpResult = Sets.newHashSet();
            for (Set<Node> p : candidates) {
                if (node.neighbor.containsAll(p)) {
                    SortedSet<Node> newCand = Sets.newTreeSet(p);
                    newCand.add(node);
                    tmpResult.add(newCand);
                }
            }

            SortedSet<Node> t = Sets.newTreeSet();
            t.add(node);
            tmpResult.add(t);

            candidates.addAll(tmpResult);
        }

        List<SortedSet<Node>> sortedCands = Lists.newArrayList(candidates);
        Collections.sort(sortedCands, new Comparator<SortedSet<Node>>() {
            @Override
            public int compare(SortedSet<Node> o1, SortedSet<Node> o2) {
                return o2.size() - o1.size();
            }
        });

        List<List<String>> results = Lists.newArrayList();
        final Set<Node> usedCols = Sets.newHashSet();

        Iterator<SortedSet<Node>> candIter = sortedCands.iterator();
        while (candIter.hasNext()) {
            SortedSet<Node> bestCandidate = candIter.next();

            if (CollectionUtils.containsAny(bestCandidate, usedCols)) {
                continue;
            }

            if (bestCandidate.size() <= 1) {
                continue;
            }

            results.addAll(splitWithSizeLimit(bestCandidate));
            usedCols.addAll(bestCandidate);
        }

        return results;
    }

    private List<List<String>> splitWithSizeLimit(Set<Node> list) {
        List<List<String>> results = Lists.newArrayList();

        List<String> currentResult = Lists.newArrayList();
        for (Node s : list) {
            if (currentResult.size() >= smartConfig.getJointColNumMax()) {
                results.add(currentResult);
                currentResult = Lists.newArrayList();
            }
            currentResult.add(s.value);
        }

        if (currentResult.size() > 1) {
            results.add(currentResult);
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

        @Override
        public int compareTo(Node o) {
            int comp = this.value.compareTo(o.value);
            return comp;
        }
    }
}
