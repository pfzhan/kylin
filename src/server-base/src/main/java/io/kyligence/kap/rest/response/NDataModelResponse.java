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
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.kyligence.kap.metadata.acl.NDataModelAclParams;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.rest.constant.ModelStatusToDisplayEnum;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class NDataModelResponse extends NDataModel {

    @JsonProperty("status")
    private ModelStatusToDisplayEnum status;
    @JsonProperty("last_build_end")
    private String lastBuildEnd;

    @JsonProperty("storage")
    private long storage;

    @JsonProperty("source")
    private long source;

    @JsonProperty("expansion_rate")
    private String expansionrate;

    @JsonProperty("usage")
    private long usage;

    @JsonProperty("root_fact_table_deleted")
    private boolean rootFactTableDeleted = false;

    @JsonProperty("segments")
    private List<NDataSegmentResponse> segments;

    @JsonProperty("recommendations_count")
    private int recommendationsCount;

    @JsonProperty("available_indexes_count")
    private long availableIndexesCount;

    @JsonProperty("empty_indexes_count")
    private long emptyIndexesCount;

    @JsonProperty("segment_holes")
    private List<SegmentRange> segmentHoles;

    @JsonProperty("total_indexes")
    private long totalIndexes;

    private long lastModify;

    public NDataModelResponse() {
        super();
    }

    public NDataModelResponse(NDataModel dataModel) {
        super(dataModel);
        this.setConfig(dataModel.getConfig());
        this.setProject(dataModel.getProject());
        this.setMvcc(dataModel.getMvcc());
        this.lastModify = lastModified;
    }

    @JsonProperty("simplified_dimensions")
    public List<NamedColumn> getNamedColumns() {
        return getAllNamedColumns().stream().filter(NamedColumn::isDimension).collect(Collectors.toList());
    }

    @JsonProperty("all_measures")
    public List<Measure> getMeasures() {
        return getAllMeasures().stream().filter(m -> !m.isTomb()).collect(Collectors.toList());
    }

    @JsonProperty("model_broken")
    public boolean isModelBroken() {
        return this.isBroken();
    }

    @JsonProperty("simplified_tables")
    public List<SimplifiedTableResponse> getSimpleTables() {
        List<SimplifiedTableResponse> simpleTables = new ArrayList<>();
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
            if (measure.isTomb()) {
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
            // get column cardinality
            final TableExtDesc.ColumnStats columnStats = tableExtDesc.getColumnStatsByName(columnDesc.getName());
            if (columnStats != null) {
                simplifiedColumnResponse.setCardinality(columnStats.getCardinality());
            }

            columns.add(simplifiedColumnResponse);
        }
        return columns;
    }

    /**
     * for 3x rest api
     */
    @JsonUnwrapped
    @Getter
    @Setter
    private NDataModelOldParams oldParams;

    @JsonUnwrapped
    @Getter
    @Setter
    private NDataModelAclParams aclParams;
}