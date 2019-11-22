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

import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.IStorageAware;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import org.apache.kylin.rest.msg.MsgPicker;
import org.springframework.validation.FieldError;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class CreateTableIndexRequest implements Validation {

    private Long id;

    private String project;

    @JsonProperty("model_id")
    private String modelId;

    private String name;

    @JsonProperty("col_order")
    private List<String> colOrder;

    @JsonProperty("layout_override_indexes")
    @Builder.Default
    private Map<String, String> layoutOverrideIndexes = Maps.newHashMap();

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
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val indexPlan = indexPlanManager.getIndexPlan(modelId);
        return indexPlan.getWhitelistLayouts().stream().filter(l -> l.getId() >= IndexEntity.TABLE_INDEX_START_ID)
                .filter(l -> !Objects.equals(l.getId(), id)).map(LayoutEntity::getName).filter(Objects::nonNull)
                .anyMatch(x -> Objects.equals(x, name));

    }

    @Override
    public String getErrorMessage(List<FieldError> errors) {
        val message = MsgPicker.getMsg();
        if (!CollectionUtils.isEmpty(errors) && errors.size() > 0) {
            if (errors.get(0).getField().equalsIgnoreCase("nameExisting")) {
                return String.format(message.getDUPLICATE_TABLE_INDEX_NAME(), name);
            }
        }
        return "";
    }
}
