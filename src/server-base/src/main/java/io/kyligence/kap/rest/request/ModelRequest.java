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
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.insensitive.ModelInsensitiveRequest;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.OptimizeRecommendation;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ModelRequest extends NDataModel implements ModelInsensitiveRequest {

    @JsonProperty("project")
    private String project;

    @JsonProperty("start")
    private String start;

    @JsonProperty("end")
    private String end;

    @JsonProperty("dimensions")
    private List<NamedColumn> dimensions = Lists.newArrayList();

    @JsonProperty("simplified_measures")
    private List<SimplifiedMeasure> simplifiedMeasures = Lists.newArrayList();

    @JsonProperty("simplified_dimensions")
    private List<NamedColumn> simplifiedDimensions = Lists.newArrayList();

    @JsonProperty("recommendation")
    private OptimizeRecommendation recommendation;

    @JsonProperty("index_plan")
    private IndexPlan indexPlan;

    @JsonProperty("save_only")
    private boolean saveOnly = false;

    private transient BiFunction<TableDesc, Boolean, Collection<ColumnDesc>> columnsFetcher = TableRef::filterColumns;

    public ModelRequest() {
        super();
    }

    public ModelRequest(NDataModel dataModel) {
        super(dataModel);
    }

}
