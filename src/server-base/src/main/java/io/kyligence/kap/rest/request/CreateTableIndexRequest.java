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
import java.util.Map;
import java.util.Objects;

import javax.validation.constraints.AssertFalse;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.IStorageAware;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import io.kyligence.kap.cube.model.NCubePlanManager;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class CreateTableIndexRequest {

    private Long id;

    private String project;

    private String model;

    private String name;

    @JsonProperty("col_order")
    private List<String> colOrder;

    @JsonProperty("layout_override_indices")
    @Builder.Default
    private Map<String, String> layoutOverrideIndices = Maps.newHashMap();

    @JsonProperty("shard_by_columns")
    private List<String> shardByColumns;

    @JsonProperty("sort_by_columns")
    private List<String> sortByColumns;

    @JsonProperty("storage_type")
    @Builder.Default
    private int storageType = IStorageAware.ID_NDATA_STORAGE;

    @Builder.Default
    @JsonProperty("load_data")
    private boolean isLoadData = true;

    @AssertFalse
    public boolean isNameExisting() {
        val cubePlanManager = NCubePlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val cubePlan = cubePlanManager.findMatchingCubePlan(model);
        return cubePlan.getWhitelistCuboidLayouts().stream().filter(l -> l.getId() >= NCuboidDesc.TABLE_INDEX_START_ID)
                .filter(l -> !Objects.equals(l.getId(), id)).map(NCuboidLayout::getName).filter(Objects::nonNull)
                .anyMatch(x -> Objects.equals(x, name));

    }
}
