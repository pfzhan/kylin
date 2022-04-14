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

package io.kyligence.kap.rest.request;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.insensitive.ModelInsensitiveRequest;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.util.scd2.SimplifiedJoinTableDesc;
import io.kyligence.kap.rest.response.LayoutRecDetailResponse;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import io.kyligence.kap.rest.util.SCD2SimplificationConvertUtil;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper = false)
public class ModelRequest extends NDataModel implements ModelInsensitiveRequest {

    @JsonProperty("project")
    private String project;

    @JsonProperty("start")
    private String start;

    @JsonProperty("end")
    private String end;

    @JsonProperty("simplified_measures")
    private List<SimplifiedMeasure> simplifiedMeasures = Lists.newArrayList();

    @JsonProperty("simplified_dimensions")
    private List<NamedColumn> simplifiedDimensions = Lists.newArrayList();

    // non-dimension columns, used for sync column alias
    // if not present, use original column name
    @JsonProperty("other_columns")
    private List<NamedColumn> otherColumns = Lists.newArrayList();

    @JsonProperty("rec_items")
    private List<LayoutRecDetailResponse> recItems = Lists.newArrayList();

    @JsonProperty("index_plan")
    private IndexPlan indexPlan;

    @JsonProperty("save_only")
    private boolean saveOnly = false;

    @JsonProperty("with_segment")
    private boolean withEmptySegment = true;

    @JsonProperty("with_model_online")
    private boolean withModelOnline = false;

    @JsonProperty("with_base_index")
    private boolean withBaseIndex = false;

    @JsonProperty("with_second_storage")
    private boolean withSecondStorage = false;

    private List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs;

    @JsonProperty("join_tables")
    public void setSimplifiedJoinTableDescs(List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs) {
        this.simplifiedJoinTableDescs = simplifiedJoinTableDescs;
        this.setJoinTables(SCD2SimplificationConvertUtil.convertSimplified2JoinTables(simplifiedJoinTableDescs));
    }

    @JsonProperty("join_tables")
    public List<SimplifiedJoinTableDesc> getSimplifiedJoinTableDescs() {
        return simplifiedJoinTableDescs;
    }

    @JsonSetter("dimensions")
    public void setDimensions(List<NamedColumn> dimensions) {
        setSimplifiedDimensions(dimensions);
    }

    @JsonSetter("all_measures")
    public void setMeasures(List<Measure> inputMeasures) {
        List<Measure> measures = inputMeasures != null ? inputMeasures : Lists.newArrayList();
        List<SimplifiedMeasure> simpleMeasureList = Lists.newArrayList();
        for (NDataModel.Measure measure : measures) {
            SimplifiedMeasure simplifiedMeasure = SimplifiedMeasure.fromMeasure(measure);
            simpleMeasureList.add(simplifiedMeasure);
        }
        setAllMeasures(measures);
        setSimplifiedMeasures(simpleMeasureList);
    }

    private transient BiFunction<TableDesc, Boolean, Collection<ColumnDesc>> columnsFetcher = TableRef::filterColumns;

    public ModelRequest() {
        super();
    }

    public ModelRequest(NDataModel dataModel) {
        super(dataModel);
        this.setSimplifiedJoinTableDescs(
                SCD2SimplificationConvertUtil.simplifiedJoinTablesConvert(dataModel.getJoinTables()));
    }

}
