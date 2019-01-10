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

package io.kyligence.kap.engine.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.PartitionDesc.IPartitionConditionBuilder;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NCubeJoinedFlatTableDesc;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelFlatTableDesc;
import io.kyligence.kap.query.util.KapQueryUtil;

public class NJoinedFlatTable {

    /*
     * Convert IJoinedFlatTableDesc to Dataset
     */

    public static Dataset<Row> generateDataset(NDataModel model, SparkSession ss) {
        TableDesc rootFactDesc = model.getRootFactTable().getTableDesc();
        Dataset<Row> ds = SourceFactory.createEngineAdapter(rootFactDesc, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(rootFactDesc, ss, Maps.newHashMap()).alias(model.getRootFactTable().getAlias());

        ds = changeSchemaToAliasDotName(ds, model.getRootFactTable().getAlias());

        for (JoinTableDesc lookupDesc : model.getJoinTables()) {
            JoinDesc join = lookupDesc.getJoin();
            if (join != null && !StringUtils.isEmpty(join.getType())) {
                String joinType = join.getType().toUpperCase();
                TableRef dimTable = lookupDesc.getTableRef();
                Dataset<Row> dimDataset = SourceFactory
                        .createEngineAdapter(dimTable.getTableDesc(), NSparkCubingEngine.NSparkCubingSource.class)
                        .getSourceData(dimTable.getTableDesc(), ss, Maps.newHashMap()).alias(dimTable.getAlias());
                dimDataset = changeSchemaToAliasDotName(dimDataset, dimTable.getAlias());

                TblColRef[] pk = join.getPrimaryKeyColumns();
                TblColRef[] fk = join.getForeignKeyColumns();
                if (pk.length != fk.length) {
                    throw new RuntimeException("Invalid join condition of lookup table:" + lookupDesc);
                }

                Column joinCond = null;
                for (int i = 0; i < pk.length; i++) {
                    Column thisJoinCond = ds.col(NSparkCubingUtil.convertFromDot(fk[i].getIdentity()))
                            .equalTo(dimDataset.col(NSparkCubingUtil.convertFromDot(pk[i].getIdentity())));
                    if (joinCond == null)
                        joinCond = thisJoinCond;
                    else
                        joinCond = joinCond.and(thisJoinCond);
                }
                ds = ds.join(dimDataset, joinCond, joinType);
            }
        }

        return ds;
    }

    public static Dataset<Row> generateDataset(IJoinedFlatTableDesc flatTable, SparkSession ss) {
        NDataModel model = flatTable.getDataModel();
        Dataset<Row> ds = generateDataset(model, ss);

        if (StringUtils.isNotBlank(model.getFilterCondition())) {
            String afterConvertCondition = replaceDot(model.getFilterCondition(), model);
            ds = ds.where(afterConvertCondition);
        }

        PartitionDesc partDesc = model.getPartitionDesc();
        if (partDesc != null && partDesc.getPartitionDateColumn() != null) {
            @SuppressWarnings("rawtypes")
            SegmentRange segRange = flatTable.getSegRange();
            if (segRange != null && !segRange.isInfinite()) {
                String afterConvertPartition = replaceDot(
                        partDesc.getPartitionConditionBuilder().buildDateRangeCondition(partDesc, null, segRange),
                        model);
                ds = ds.where(afterConvertPartition);// TODO: mp not supported right now
            }
        }
        
        if (flatTable instanceof NDataModelFlatTableDesc) {
            return selectNModelJoinedFlatTable(ds, (NDataModelFlatTableDesc) flatTable);
        }

        if (flatTable instanceof NCubeJoinedFlatTableDesc) {
            return selectNCubeJoinedFlatTable(ds, (NCubeJoinedFlatTableDesc) flatTable);
        }

        return ds;
    }

    public static Dataset<Row> selectNModelJoinedFlatTable(Dataset<Row> ds, NDataModelFlatTableDesc flatTable) {
        List<TblColRef> colRefs = flatTable.getAllColumns();
        String[] exprs = new String[colRefs.size()];
        String[] names = new String[exprs.length];
        for (int i = 0; i < exprs.length; i++) {
            exprs[i] = NSparkCubingUtil.convertFromDot(colRefs.get(i).getExpressionInSourceDB());
            names[i] = NSparkCubingUtil.convertFromDot(colRefs.get(i).getIdentity());
        }
        return ds.selectExpr(exprs).toDF(names);
    }

    public static Dataset<Row> selectNCubeJoinedFlatTable(Dataset<Row> ds, NCubeJoinedFlatTableDesc flatTable) {
        List<TblColRef> colRefs = flatTable.getAllColumns();
        List<Integer> colIndices = flatTable.getIndices();
        String[] exprs = new String[colRefs.size()];
        String[] indices = new String[exprs.length];
        for (int i = 0; i < exprs.length; i++) {
            exprs[i] = NSparkCubingUtil.convertFromDot(colRefs.get(i).getExpressionInSourceDB());
            indices[i] = String.valueOf(colIndices.get(i));
        }
        return ds.selectExpr(exprs).toDF(indices);
    }

    public static String replaceDot(String original, NDataModel model) {
        StringBuilder sb = new StringBuilder(original);
        for (NDataModel.NamedColumn namedColumn : model.getAllNamedColumns()) {
            int start = 0;
            String aliasDotColumn = namedColumn.getAliasDotColumn();
            while ((start = sb.toString().toLowerCase().indexOf(aliasDotColumn.toLowerCase())) != -1) {
                sb.replace(start, start + aliasDotColumn.length(),
                        NSparkCubingUtil.convertFromDot(aliasDotColumn));
            }
        }
        return sb.toString();
    }

    public static Dataset<Row> changeSchemaToAliasDotName(Dataset<Row> original, String alias) {
        StructField[] sf = original.schema().fields();
        String[] newSchema = new String[sf.length];

        for (int i = 0; i < newSchema.length; i++)
            newSchema[i] = NSparkCubingUtil.convertFromDot(alias + "." + sf[i].name());

        return original.toDF(newSchema);
    }
    

    /*
     * Convert IJoinedFlatTableDesc to SQL statement
     */
    
    public static String generateSelectDataStatement(IJoinedFlatTableDesc flatDesc, boolean singleLine,
            String[] skipAs) {
        final String sep = singleLine ? " " : "\n";
        final List<String> skipAsList = (skipAs == null) ? new ArrayList<String>() : Arrays.asList(skipAs);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + sep);

        for (int i = 0; i < flatDesc.getAllColumns().size(); i++) {
            TblColRef col = flatDesc.getAllColumns().get(i);
            if (i > 0) {
                sql.append(",");
            }
            String colTotalName = String.format("%s.%s", col.getTableRef().getTableName(), col.getName());
            if (skipAsList.contains(colTotalName)) {
                sql.append(col.getExpressionInSourceDB() + sep);
            } else {
                sql.append(col.getExpressionInSourceDB() + " as " + colName(col) + sep);
            }
        }
        appendJoinStatement(flatDesc, sql, singleLine);
        appendWhereStatement(flatDesc, sql, singleLine);
        return sql.toString();
    }

    public static void appendJoinStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, boolean singleLine) {
        KapQueryUtil.appendJoinStatement(flatDesc.getDataModel(), sql, singleLine);
    }

    private static void appendWhereStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, boolean singleLine) {
        final String sep = singleLine ? " " : "\n";

        StringBuilder whereBuilder = new StringBuilder();
        whereBuilder.append("WHERE 1=1");

        NDataModel model = flatDesc.getDataModel();
        if (StringUtils.isNotEmpty(model.getFilterCondition())) {
            whereBuilder.append(" AND (").append(model.getFilterCondition()).append(") ");
        }

        PartitionDesc partDesc = model.getPartitionDesc();
        SegmentRange segRange = flatDesc.getSegRange();
        if (flatDesc.getSegment() != null //
                && partDesc != null && partDesc.getPartitionDateColumn() != null //
                && segRange != null && !segRange.isInfinite()) {
            
            IPartitionConditionBuilder builder = flatDesc.getDataModel().getPartitionDesc().getPartitionConditionBuilder();
            if (builder != null) {
                whereBuilder.append(" AND (");
                whereBuilder.append(builder.buildDateRangeCondition(partDesc, flatDesc.getSegment(), segRange));
                whereBuilder.append(")" + sep);
            }
        }

        sql.append(whereBuilder.toString());
    }

    private static String colName(TblColRef col) {
        return col.getTableAlias() + "_" + col.getName();
    }
}
