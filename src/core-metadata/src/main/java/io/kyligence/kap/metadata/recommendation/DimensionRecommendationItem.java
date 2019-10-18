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
package io.kyligence.kap.metadata.recommendation;

import java.io.Serializable;

import org.apache.kylin.common.util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.var;

public class DimensionRecommendationItem implements Serializable, RecommendationItem<DimensionRecommendationItem> {
    @Getter
    @Setter
    @JsonProperty("item_id")
    private long itemId;

    @Getter
    @Setter
    @JsonProperty("column")
    private NDataModel.NamedColumn column;

    @Getter
    @Setter
    @JsonProperty("recommendation_type")
    private RecommendationType recommendationType = RecommendationType.ADDITION;

    @Getter
    @Setter
    @JsonProperty("is_auto_change_name")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private boolean isAutoChangeName = true;

    @Getter
    @Setter
    @JsonProperty("data_type")
    private String dataType;

    @Getter
    @Setter
    @JsonIgnore
    private boolean copy = false;

    public void apply(OptimizeContext context, boolean real) {
        if (context.getDeletedDimensionRecommendations().contains(itemId)) {
            return;
        }
        var item = context.getDimensionRecommendationItem(itemId);
        val column = item.column;
        Preconditions.checkNotNull(column);

        val modelColumn = context.getVirtualIdColumnMap().get(column.getId());
        if (modelColumn == null || !modelColumn.isExist()) {
            if (!real) {
                context.deleteDimensionRecommendationItem(itemId);
                return;
            } else {
                throw new PassConflictException("cc item id " + itemId + "column not exists in model");
            }
        }
        if (modelColumn.isDimension()) {
            //dimension is already in model
            context.deleteDimensionRecommendationItem(itemId);
            return;
        }
        modelColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        if (!column.getName().equals(modelColumn.getName())) {
            context.getDimensionColumnNameIdMap().remove(modelColumn.getName());
        }

        modelColumn.setName(column.getName());
        context.getDimensionColumnNameIdMap().put(column.getName(), column.getId());
    }

    @Override
    public void checkDependencies(OptimizeContext context, boolean real) {
        if (context.getDeletedDimensionRecommendations().contains(itemId)) {
            return;
        }
        val recommendation = context.getDimensionRecommendationItem(itemId);
        val columns = context.getVirtualIdColumnMap();
        val id = recommendation.getColumn().getId();
        if (!columns.containsKey(id) && !real) {
            context.failDimensionRecommendationItem(itemId);
            return;
        }
        if (!columns.containsKey(id) || (real && OptimizeRecommendationManager.isVirtualColumnId(id))) {
            throw new DependencyLostException(String.format(
                    "dimension lost dependency: column %s not exists in all columns, you may need pass it first",
                    context.getDimensionRecommendationItem(itemId).getColumn().getName()));
        }

    }

    @Override
    public DimensionRecommendationItem copy() {
        if (this.isCopy()) {
            return this;
        }
        val res = JsonUtil.deepCopyQuietly(this, DimensionRecommendationItem.class);
        res.setCopy(true);
        return res;
    }

    @Override
    public void translate(OptimizeContext context) {
        val item = context.getDimensionRecommendationItem(itemId);
        if (context.getTranslations().containsKey(item.getColumn().getId())) {
            context.copyDimensionRecommendationItem(itemId).getColumn().setId(
                    context.getTranslations().get(context.getDimensionRecommendationItem(itemId).getColumn().getId()));
        }
        for (val pair : context.getNameTranslations()) {
            val recommendation = context.getDimensionRecommendationItem(itemId);
            val oldName = pair.getFirst();
            val newName = pair.getSecond();
            val tableAndName = recommendation.getColumn().getAliasDotColumn().split("\\.");
            if (tableAndName[0].equals(context.getFactTableName()) && tableAndName[1].equals(oldName)) {
                val copy = context.copyDimensionRecommendationItem(itemId);
                copy.getColumn().setAliasDotColumn(tableAndName[0] + "." + newName);
                break;
            }
        }

    }
}
