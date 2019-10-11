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

import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.rest.util.PagingUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.RecommendationType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

@Getter
@Setter
@NoArgsConstructor
public class TableIndexRecommendationResponse {
    @JsonProperty("id")
    private long id;

    @JsonProperty("item_id")
    private long itemId;

    @JsonProperty("columns")
    private List<String> columns = Lists.newArrayList();

    @JsonProperty("shard_by_columns")
    private List<String> shardByColumns = Lists.newArrayList();

    @JsonProperty("recommendation_type")
    private RecommendationType recommendationType;

    @JsonProperty("size")
    private int size;

    public TableIndexRecommendationResponse(LayoutEntity layoutEntity, NDataModel optimizedModel, String content, int pageOffset, int pageSize) {
        this.id = layoutEntity.getId();
        val columnIdMap = optimizedModel.getAllNamedColumns().stream()
                .collect(toMap(NDataModel.NamedColumn::getId, NDataModel.NamedColumn::getAliasDotColumn));

        layoutEntity.getColOrder().forEach(id -> {
            val columnName = columnIdMap.get(id);
            if (StringUtils.isNotEmpty(content) && !columnName.contains(content.trim().toUpperCase()))
                return;
            columns.add(columnName);
        });
        layoutEntity.getShardByColumns().forEach(id -> shardByColumns.add(columnIdMap.get(id)));

        this.size = this.columns.size();
        this.columns = PagingUtil.cutPage(columns, pageOffset, pageSize);
        this.shardByColumns = this.shardByColumns.stream().filter(column -> columns.contains(column)).collect(Collectors.toList());
    }
}
