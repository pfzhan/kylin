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

package io.kyligence.kap.query.engine.view;

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.JoinedFlatTable;
import io.kyligence.kap.metadata.model.NDataModel;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ModelViewGenerator {

    private final NDataModel model;

    public ModelViewGenerator(NDataModel model) {
        this.model = model;
    }

    public String generateViewSQL() {
        return JoinedFlatTable.generateSelectDataStatement(
                model, Lists.newArrayList(listModelViewColumns()),
                true, true, false, this::getColumnNameFromModel).trim();
    }

    private Set<TblColRef> listModelViewColumns() {
        Set<TblColRef> colRefs = new HashSet<>();

        // add all dims
        model.getEffectiveDimensions().forEach((id, colRef) -> {
            colRefs.add(colRef);
        });

        // add all measure source columns
        model.getEffectiveMeasures().forEach((id, measure) ->
                colRefs.addAll(measure.getFunction().getColRefs()));



        // add all cc source columns
        List<TblColRef> ccCols = colRefs.stream()
                .filter(col -> col.getColumnDesc().isComputedColumn())
                .collect(Collectors.toList());
        colRefs.addAll(getComputedColumnSourceColumns(ccCols));

        return colRefs;
    }


    private String getColumnNameFromModel(TblColRef colRef) {
        Integer id = model.getEffectiveCols().inverse().get(colRef);
        return id == null ? null : model.getNameByColumnId(id).toUpperCase(Locale.getDefault());
    }

    /**
     * parse cc expr and find all table columns ref
     * @param ccCols
     * @return
     */
    private Set<TblColRef> getComputedColumnSourceColumns(List<TblColRef> ccCols) {
        List<String> ccExprs = ccCols.stream()
                .map(colRef -> model.findCCByCCColumnName(colRef.getName()))
                .filter(Objects::nonNull)
                .map(ComputedColumnDesc::getExpression).collect(Collectors.toList());
                
        try {
            SqlSelect select = (SqlSelect)
                    CalciteParser.parse("select " + String.join(",", ccExprs),
                            this.model != null ? this.model.getProject() : null);

            return getAllIdentifiers(select).stream()
                    .map(SqlIdentifier::toString)
                    .map(model::getColRef)
                    .collect(Collectors.toSet());
        } catch (SqlParseException e) {
            return new HashSet<>();
        }
    }

    private static List<SqlIdentifier> getAllIdentifiers(SqlNode sqlNode) {
        if (sqlNode instanceof SqlNodeList) {
            return getAllIdentifiersFromList(((SqlNodeList) sqlNode).getList());
        } else if (sqlNode instanceof SqlCall) {
            return getAllIdentifiersFromList(((SqlCall) sqlNode).getOperandList());
        } else if (sqlNode instanceof SqlIdentifier) {
            return Lists.newArrayList((SqlIdentifier) sqlNode);
        } else {
            return Lists.newArrayList();
        }
    }

    private static List<SqlIdentifier> getAllIdentifiersFromList(List<SqlNode> nodes) {
        return nodes.stream().map(ModelViewGenerator::getAllIdentifiers)
                .flatMap(Collection::stream).collect(Collectors.toList());
    }
}
