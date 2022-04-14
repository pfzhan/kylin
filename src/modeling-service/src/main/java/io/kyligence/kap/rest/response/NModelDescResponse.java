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

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.metadata.model.JoinTableDesc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class NModelDescResponse implements Serializable {
    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("last_modified")
    private long lastModified;
    @JsonProperty("create_time")
    private long createTime;
    @JsonProperty("version")
    private String version;
    @JsonProperty("name")
    private String name;
    @JsonProperty("project")
    private String project;
    @JsonProperty("description")
    private String description;
    @JsonProperty("dimensions")
    private List<Dimension> dimensions;
    @JsonProperty("measures")
    private List<NDataModel.Measure> measures;
    @JsonProperty("aggregation_groups")
    private List<AggGroupResponse> aggregationGroups;
    @JsonProperty("computed_columns")
    private List<ComputedColumnDesc> computedColumnDescs;
    @JsonProperty("fact_table")
    private String factTable;
    @JsonProperty("join_tables")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<JoinTableDesc> joinTables;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Dimension implements Serializable {

        @JsonUnwrapped
        private NDataModel.NamedColumn namedColumn;

        @JsonProperty("derived")
        private boolean derived = false;
    }
}
