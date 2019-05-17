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

package io.kyligence.kap.rest.response;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class NDataModelResponse extends NDataModel {

    @JsonProperty("status")
    private RealizationStatusEnum status;
    @JsonProperty("last_build_end")
    private String lastBuildEnd;

    @JsonProperty("storage")
    private long storage;

    @JsonProperty("usage")
    private long usage;

    public NDataModelResponse() {
        super();
    }

    public NDataModelResponse(NDataModel dataModel) {
        super(dataModel);
        this.setConfig(dataModel.getConfig());
        this.setProject(dataModel.getProject());
    }

    @JsonProperty("simplified_dimensions")
    public List<NamedColumn> getNamedColumns() {
        return getAllNamedColumns().stream().filter(NamedColumn::isDimension).collect(Collectors.toList());
    }

    @JsonProperty("all_measures")
    public List<Measure> getMeasures() {
        return getAllMeasures().stream().filter(m -> !m.tomb).collect(Collectors.toList());
    }

    @JsonProperty("simplified_tables")
    public List<SimplifiedTableResponse> getSimpleTables() {
        List<SimplifiedTableResponse> simpleTables = new ArrayList<>();
        if (RealizationStatusEnum.BROKEN.equals(status)) {
            return simpleTables;
        }
        for (TableRef tableRef : getAllTables()) {
            SimplifiedTableResponse simpleTable = new SimplifiedTableResponse();
            simpleTable.setTable(tableRef.getTableIdentity());
            List<SimplifiedColumnResponse> columns = getSimplifiedColumns(tableRef);
            simpleTable.setColumns(columns);
            simpleTables.add(simpleTable);
        }
        return simpleTables;
    }

    @JsonProperty("simplified_measures")
    public List<SimplifiedMeasure> getSimplifiedMeasures() {
        List<NDataModel.Measure> measures = getAllMeasures();
        List<SimplifiedMeasure> measureResponses = new ArrayList<>();
        for (NDataModel.Measure measure : measures) {
            if (measure.tomb) {
                continue;
            }
            measureResponses.add(SimplifiedMeasure.fromMeasure(measure));
        }
        return measureResponses;
    }

    private List<SimplifiedColumnResponse> getSimplifiedColumns(TableRef tableRef) {
        List<SimplifiedColumnResponse> columns = new ArrayList<>();
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getConfig(), getProject());
        for (ColumnDesc columnDesc : tableRef.getTableDesc().getColumns()) {
            TableExtDesc tableExtDesc = tableMetadataManager.getOrCreateTableExt(tableRef.getTableDesc());
            SimplifiedColumnResponse simplifiedColumnResponse = new SimplifiedColumnResponse();
            simplifiedColumnResponse.setName(columnDesc.getName());
            simplifiedColumnResponse.setComment(columnDesc.getComment());
            simplifiedColumnResponse.setDataType(columnDesc.getDatatype());
            simplifiedColumnResponse.setComputedColumn(columnDesc.isComputedColumn());
            //get column cardinality
            List<TableExtDesc.ColumnStats> columnStats = tableExtDesc.getColumnStats();
            for (TableExtDesc.ColumnStats columnStat : columnStats) {
                if (columnStat.getColumnName().equals(columnDesc.getName())) {
                    simplifiedColumnResponse.setCardinality(columnStat.getCardinality());
                }
            }
            columns.add(simplifiedColumnResponse);
        }
        return columns;
    }
}