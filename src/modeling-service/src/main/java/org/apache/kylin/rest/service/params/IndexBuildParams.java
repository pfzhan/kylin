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

package org.apache.kylin.rest.service.params;

import java.util.List;

import org.apache.kylin.job.dao.ExecutablePO;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndexBuildParams {

    private String project;

    private String modelId;

    private List<String> segmentIds;

    private List<Long> layoutIds;

    /**
     * should delete to be deleted layouts or not, default true
     */
    @Builder.Default
    private boolean deleteTBDLayouts = true;

    private boolean parallelBuildBySegment;

    @Builder.Default
    private int priority = ExecutablePO.DEFAULT_PRIORITY;

    private boolean partialBuild;

    private String yarnQueue;

    private Object tag;
}
