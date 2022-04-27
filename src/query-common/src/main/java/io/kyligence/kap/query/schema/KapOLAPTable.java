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

package io.kyligence.kap.query.schema;

import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.schema.OLAPSchema;
import org.apache.kylin.query.schema.OLAPTable;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.query.relnode.KapTableScan;

/**
 */
public class KapOLAPTable extends OLAPTable {

    public KapOLAPTable(OLAPSchema schema, TableDesc tableDesc, Map<String, List<NDataModel>> modelsMap) {
        super(schema, tableDesc, modelsMap);
    }

    @Override
    public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
        int fieldCount = relOptTable.getRowType().getFieldCount();
        int[] fields = identityList(fieldCount);
        return new KapTableScan(context.getCluster(), relOptTable, this, fields);
    }
}
