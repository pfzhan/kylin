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

import java.util.List;

import io.kyligence.kap.metadata.model.Canvas;
import io.kyligence.kap.rest.response.SimplifiedMeasure;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Data;

@Data
public class ModelSemanticUpdateRequest {

    private String project;

    private String name;

    @JsonProperty("join_tables")
    private List<JoinTableDesc> joinTables;

    @JsonProperty("partition_desc")
    private PartitionDesc partitionDesc;

    @JsonProperty("all_named_columns")
    private List<NDataModel.NamedColumn> allDimensions = Lists.newArrayList();

    @JsonProperty("simplified_measures")
    private List<SimplifiedMeasure> simplifiedMeasures;

    @JsonProperty("multilevel_partition_cols")
    private List<String> mpColStrs = Lists.newArrayList();

    @JsonProperty("computed_columns")
    private List<ComputedColumnDesc> computedColumnDescs = Lists.newArrayList();

    @JsonProperty("canvas")
    private Canvas canvas;

}
