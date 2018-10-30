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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class JoinsGraph implements Serializable {
    private TableRef center = null;

    private Set<TableRef> nodes = new HashSet<>();
    private Set<Edge> edges = new HashSet<>();
    private Map<TableRef, List<Edge>> node2Edges = new HashMap<>();

    private static class Edge implements Serializable {
        TableRef left;
        TableRef right;
        boolean isLeftJoin;

        ColumnDesc[] leftCols;
        ColumnDesc[] rightCols;

        private Edge(JoinDesc join) {
            this.left = join.getFKSide();
            this.right = join.getPKSide();
            this.isLeftJoin = join.isLeftJoin();

            leftCols = new ColumnDesc[join.getForeignKeyColumns().length];
            int i = 0;
            for (TblColRef colRef : join.getForeignKeyColumns()) {
                leftCols[i++] = colRef.getColumnDesc();
            }

            rightCols = new ColumnDesc[join.getPrimaryKeyColumns().length];
            i = 0;
            for (TblColRef colRef : join.getPrimaryKeyColumns()) {
                rightCols[i++] = colRef.getColumnDesc();
            }
        }

        public static Edge of(JoinDesc join) {
            return new Edge(join);
        }

        public TableRef other(TableRef tableRef) {
            if (left.equals(tableRef)) {
                return right;
            } else if (right.equals(tableRef)) {
                return left;
            }

            throw new IllegalArgumentException("table " + tableRef + " is not on the edge " + this);
        }

        /**
         * compare:
         * 1. JoinType
         * 2. Columns on both sides
         */
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Edge))
                return false;

            Edge that = (Edge) obj;

            if (this.isLeftJoin != that.isLeftJoin) {
                return false;
            }

            if (this.isLeftJoin) {
                return Arrays.equals(this.leftCols, that.leftCols) && Arrays.equals(this.rightCols, that.rightCols);
            } else {
                return (Arrays.equals(this.leftCols, that.leftCols) && Arrays.equals(this.rightCols, that.rightCols))
                        || (Arrays.equals(this.leftCols, that.rightCols)
                                && Arrays.equals(this.rightCols, that.leftCols));
            }
        }

        @Override
        public int hashCode() {
            if (this.isLeftJoin) {
                return Objects.hash(isLeftJoin, leftCols, rightCols);
            } else {
                if (Arrays.hashCode(leftCols) < Arrays.hashCode(rightCols)) {
                    return Objects.hash(isLeftJoin, leftCols, rightCols);
                } else {
                    return Objects.hash(isLeftJoin, rightCols, leftCols);
                }
            }
        }

        @Override
        public String toString() {
            return "JoinsGraph.Edge[" + isLeftJoin + "," + left + "," + right + "," + leftCols + "," + rightCols + "]";
        }
    }

    /**
     * For model there's always a center, if there's only one tableScan it's the center.
     * Otherwise the center is not determined, it's a linked graph, hard to tell the center.
     */
    public JoinsGraph(TableRef root, List<JoinDesc> joins) {
        this.center = root;
        nodes.add(root);

        for (JoinDesc join : joins) {
            for (TblColRef col : join.getForeignKeyColumns())
                Preconditions.checkState(col.isQualified());
            for (TblColRef col : join.getPrimaryKeyColumns())
                Preconditions.checkState(col.isQualified());
        }

        for (JoinDesc join : joins) {
            TableRef fkTable = join.getFKSide();
            TableRef pkTable = join.getPKSide();
            nodes.add(fkTable);
            nodes.add(pkTable);
            Edge edge = Edge.of(join);

            edges.add(edge);
            if (node2Edges.containsKey(fkTable)) {
                node2Edges.get(fkTable).add(edge);
            } else {
                node2Edges.put(fkTable, Lists.newArrayList(edge));
            }

            if (node2Edges.containsKey(pkTable)) {
                node2Edges.get(pkTable).add(edge);
            } else {
                node2Edges.put(pkTable, Lists.newArrayList(edge));
            }
        }
    }

    public static boolean match(JoinsGraph query, JoinsGraph pattern, Map<String, String> matchAlias) {
        if (pattern.center == null) {
            throw new IllegalArgumentException("pattern(model) should have a center: " + pattern);
        }

        List<TableRef> candidatesOfQCenter = query.searchNodeByIdentity(pattern.center);
        if (CollectionUtils.isEmpty(candidatesOfQCenter)) {
            return false;
        }

        Map<String, String> tmpMatchAlias = Maps.newHashMap(matchAlias);
        for (TableRef qCenter : candidatesOfQCenter) {
            matchAlias.put(qCenter.getAlias(), pattern.center.getAlias());
            if (query.edges.isEmpty()) {
                return true;
            }
            if (innerMatch(query, pattern, qCenter, pattern.center, null, null, matchAlias)) {
                return true;
            }
            matchAlias.clear();
            matchAlias.putAll(tmpMatchAlias);
        }
        return false;
    }

    public static boolean innerMatch(JoinsGraph query, JoinsGraph pattern, TableRef qVisited, TableRef pVisitied,
            final TableRef qPrev, TableRef pPrev, Map<String, String> matchAlias) {
        List<TableRef> qNexts = query.otherSide(qVisited);
        List<TableRef> pNexts = pattern.otherSide(pVisitied);

        Set<TableRef> matchedPn = new HashSet<>();
        for (TableRef qn : qNexts) {
            if (qn.equals(qPrev)) {
                continue;
            }

            boolean matched = false;
            for (TableRef pn : pNexts) {
                if (pn.equals(pPrev) || matchedPn.contains(pn)
                        || !qn.getTableIdentity().equals(pn.getTableIdentity())) {
                    continue;
                }

                Edge queryEdge = query.getEdge(qVisited, qn);
                Edge patternEdge = pattern.getEdge(pVisitied, pn);
                if (queryEdge == null || !queryEdge.equals(patternEdge) || (matchAlias.containsKey(qn.getAlias())
                        && !matchAlias.get(qn.getAlias()).equals(pn.getAlias())))
                    continue;

                matchAlias.put(qn.getAlias(), pn.getAlias());
                matched = innerMatch(query, pattern, qn, pn, qVisited, pVisitied, matchAlias);
                if (matched) {
                    matchedPn.add(pn);
                    break;
                }
            }

            // any unmatched child node means unmatched graph
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    private List<TableRef> searchNodeByIdentity(final TableRef table) {
        // special case: several same nodes in a JoinGraph
        List<TableRef> candidatesOfQueryRoot = Lists.newArrayList();
        for (TableRef tbl : nodes) {
            if (tbl.getTableIdentity().equals(table.getTableIdentity())) {
                candidatesOfQueryRoot.add(tbl);
            }
        }
        return candidatesOfQueryRoot;
    }

    public List<TableRef> otherSide(TableRef thisSide) {
        List<Edge> edges = node2Edges.get(thisSide);
        if (edges == null) {
            return Lists.newArrayList();
        }

        List<TableRef> result = Lists.newArrayList();
        for (Edge e : edges) {
            result.add(e.other(thisSide));
        }

        return result;
    }

    public Edge getEdge(TableRef one, TableRef two) {
        List<Edge> edges = node2Edges.get(one);

        for (Edge e : edges) {
            if (e.other(one).equals(two)) {
                return e;
            }
        }

        return null;
    }
}