/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.response;

import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class ExcludedTableDetailResponse {
    @JsonProperty("table")
    private String table;
    @JsonProperty("excluded")
    private boolean excluded;
    @JsonProperty("total_size")
    private int totalSize;
    @JsonProperty("offset")
    private int offset;
    @JsonProperty("limit")
    private int limit;
    @JsonProperty("excluded_columns")
    private List<ExcludedColumnResponse> excludedColumns = Lists.newArrayList();
    @JsonProperty("admitted_columns")
    private List<ExcludedColumnResponse> admittedColumns = Lists.newArrayList();
}
