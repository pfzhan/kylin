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
        val column = context.getDimensionRecommendationItem(itemId).column;
        Preconditions.checkNotNull(column);

        val modelColumn = context.getVirtualIdColumnMap().get(column.getId());
        Preconditions.checkNotNull(modelColumn);
        Preconditions.checkArgument(modelColumn.isExist());
        if (modelColumn.isDimension()) {
            //dimension is already in model
            context.deleteDimensionRecommendationItem(itemId);
            return;
        }
        modelColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        if (!column.getName().equals(modelColumn.getName())) {
            context.getVirtualColumnNameIdMap().remove(modelColumn.getName());
            context.getVirtualColumnNameIdMap().put(column.getName(), column.getId());
            modelColumn.setName(column.getName());
        }

        if (real) {
            val realColumn = context.getRealIdColumnMap().get(column.getId());
            realColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
            if (!column.getName().equals(realColumn.getName())) {
                context.getRealColumnNameIdMap().remove(realColumn.getName());
                context.getRealColumnNameIdMap().put(column.getName(), column.getId());
                realColumn.setName(column.getName());
            }
        }
    }

    @Override
    public void checkDependencies(OptimizeContext context, boolean real) {
        val recommendation = context.getDimensionRecommendationItem(itemId);
        val columns = real ? context.getRealIdColumnMap() : context.getVirtualIdColumnMap();
        if (!columns.containsKey(recommendation.getColumn().getId())) {
            if (!real && context.getFailCCColumn().contains(recommendation.getColumn().getAliasDotColumn())) {
                context.failDimensionRecommendationItem(itemId);
            } else {
                throw new DependencyLostException(String.format(
                        "dimension lost dependency: column %s not exists in all columns, you may need pass it first",
                        context.getDimensionRecommendationItem(itemId).getColumn().getName()));
            }
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
