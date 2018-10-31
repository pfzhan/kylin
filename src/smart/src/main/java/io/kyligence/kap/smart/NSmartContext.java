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

package io.kyligence.kap.smart;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.smart.common.SmartConfig;
import io.kyligence.kap.smart.model.ModelTree;

public class NSmartContext {
    private final KylinConfig kylinConfig;
    private final SmartConfig smartConfig;
    private final String project;
    private final String[] sqls;

    private Map<Integer, Collection<OLAPContext>> olapContexts;
    private List<NModelContext> modelContexts;

    private final NTableMetadataManager tableMetadataManager;
    private final Map<String, TableExtDesc.ColumnStats> columnStatsCache = Maps.newConcurrentMap();

    public static class NModelContext {
        private ModelTree modelTree; // query

        private NDataModel targetModel; // output model
        private NDataModel origModel; // used when update existing models

        private NCubePlan targetCubePlan;
        private NCubePlan origCubePlan;

        private NSmartContext smartContext;

        public NModelContext(NSmartContext smartContext) {
            this.smartContext = smartContext;
        }

        public NSmartContext getSmartContext() {
            return smartContext;
        }

        public ModelTree getModelTree() {
            return modelTree;
        }

        public void setModelTree(ModelTree modelTree) {
            this.modelTree = modelTree;
        }

        public NDataModel getTargetModel() {
            return targetModel;
        }

        public void setTargetModel(NDataModel targetModel) {
            this.targetModel = targetModel;
        }

        public NDataModel getOrigModel() {
            return origModel;
        }

        public void setOrigModel(NDataModel origModel) {
            this.origModel = origModel;
        }

        public NCubePlan getTargetCubePlan() {
            return targetCubePlan;
        }

        public void setTargetCubePlan(NCubePlan targetCubePlan) {
            this.targetCubePlan = targetCubePlan;
        }

        public NCubePlan getOrigCubePlan() {
            return origCubePlan;
        }

        public void setOrigCubePlan(NCubePlan origCubePlan) {
            this.origCubePlan = origCubePlan;
        }
    }

    public NSmartContext(KylinConfig kylinConfig, String project, String[] sqls) {
        this.kylinConfig = kylinConfig;
        this.project = project;
        this.sqls = sqls;
        this.smartConfig = SmartConfig.wrap(this.kylinConfig);

        tableMetadataManager = NTableMetadataManager.getInstance(this.kylinConfig, project);
    }

    public KylinConfig getKylinConfig() {
        return kylinConfig;
    }

    public SmartConfig getSmartConfig() {
        return smartConfig;
    }

    public String[] getSqls() {
        return sqls;
    }

    public String getProject() {
        return project;
    }

    public Map<Integer, Collection<OLAPContext>> getOlapContexts() {
        return olapContexts;
    }

    public void setOlapContexts(Map<Integer, Collection<OLAPContext>> olapContexts) {
        this.olapContexts = olapContexts;
    }

    public List<NModelContext> getModelContexts() {
        return modelContexts;
    }

    public void setModelContexts(List<NModelContext> modelContexts) {
        this.modelContexts = modelContexts;
    }

    // =======================

    public TableExtDesc.ColumnStats getColumnStats(TblColRef colRef) {
        TableExtDesc.ColumnStats ret = columnStatsCache.get(colRef.getIdentity());
        if (ret != null)
            return ret;

        TableExtDesc tableExtDesc = tableMetadataManager.getTableExt(colRef.getTableRef().getTableDesc());
        if (tableExtDesc != null && !tableExtDesc.getColumnStats().isEmpty()) {
            ret = tableExtDesc.getColumnStats().get(colRef.getColumnDesc().getZeroBasedIndex());
            columnStatsCache.put(colRef.getIdentity(), ret);
        } else {
            ret = null;
        }
        return ret;
    }
}
