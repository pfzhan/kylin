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
package io.kyligence.kap.metadata.model.schema;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;

import io.kyligence.kap.guava20.shaded.common.collect.MapDifference;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.guava20.shaded.common.graph.Graph;
import io.kyligence.kap.guava20.shaded.common.graph.GraphBuilder;
import io.kyligence.kap.guava20.shaded.common.graph.MutableGraph;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.val;

public class SchemaUtil {

    public static ModelDifference diff(IndexPlan sourcePlan, IndexPlan targetPlan) {
        val sourceGraph = dependencyGraph(sourcePlan);
        val targetGraph = dependencyGraph(targetPlan);
        return new ModelDifference(sourceGraph, targetGraph);
    }

    public static Graph<SchemaNode> dependencyGraph(IndexPlan plan) {
        val model = plan.getModel();
        val tables = model.getAllTables().stream().map(TableRef::getTableDesc).collect(Collectors.toList());
        return dependencyGraph(tables, Arrays.asList(plan));
    }

    public static Graph<SchemaNode> dependencyGraph(String project) {
        val tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val planManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        return dependencyGraph(tableManager.listAllTables(), dataflowManager.listUnderliningDataModels().stream()
                .map(model -> planManager.getIndexPlan(model.getId())).collect(Collectors.toList()));
    }

    static Graph<SchemaNode> dependencyGraph(List<TableDesc> tables, List<IndexPlan> plans) {
        MutableGraph<SchemaNode> graph = GraphBuilder.directed().allowsSelfLoops(false).build();
        for (TableDesc tableDesc : tables) {
            Stream.of(tableDesc.getColumns()).forEach(col -> graph.addNode(SchemaNode.ofTableColumn(col)));
        }

        for (IndexPlan plan : plans) {
            new ModelEdgeCollector(plan, graph).collect();
        }
        return graph;
    }

    public static class ModelDifference {

        private final Graph<SchemaNode> sourceGraph;

        private final Graph<SchemaNode> targetGraph;

        private final MapDifference<SchemaNode, SchemaNode> nodeDiff;

        public ModelDifference(Graph<SchemaNode> sourceGraph, Graph<SchemaNode> targetGraph) {
            this.sourceGraph = sourceGraph;
            this.targetGraph = targetGraph;

            this.nodeDiff = Maps.difference(
                    sourceGraph.nodes().stream().collect(Collectors.toMap(Function.identity(), Function.identity())),
                    targetGraph.nodes().stream().collect(Collectors.toMap(Function.identity(), Function.identity())));

        }

    }
}
