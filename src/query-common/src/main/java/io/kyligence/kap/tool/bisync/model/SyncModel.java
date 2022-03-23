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
package io.kyligence.kap.tool.bisync.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SyncModel {

    private String projectName;

    private String modelName;

    private String host;

    private String port;

    private JoinTreeNode joinTree;

    private Map<String, ColumnDef> columnDefMap;

    private List<MeasureDef> metrics;

    private Set<String[]> hierarchies;

    public JoinTreeNode getJoinTree() {
        return joinTree;
    }

    public void setJoinTree(JoinTreeNode joinTree) {
        this.joinTree = joinTree;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getModelName() {
        return modelName;
    }

    public Map<String, ColumnDef> getColumnDefMap() {
        return columnDefMap;
    }

    public void setColumnDefMap(Map<String, ColumnDef> columnDefMap) {
        this.columnDefMap = columnDefMap;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public Set<String[]> getHierarchies() {
        return hierarchies;
    }

    public void setHierarchies(Set<String[]> hierarchies) {
        this.hierarchies = hierarchies;
    }

    public List<MeasureDef> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<MeasureDef> metrics) {
        this.metrics = metrics;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

}
