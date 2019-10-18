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

import org.apache.calcite.sql.dialect.HiveSqlDialect;
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

public class MeasureRecommendationItem implements Serializable, RecommendationItem<MeasureRecommendationItem> {

    @Getter
    @Setter
    @JsonProperty("item_id")
    private long itemId;

    @Getter
    @Setter
    @JsonProperty("measure")
    private NDataModel.Measure measure;

    @Getter
    @Setter
    @JsonProperty("recommendation_type")
    private RecommendationType recommendationType = RecommendationType.ADDITION;

    @Getter
    @Setter
    @JsonProperty("measure_id")
    private int measureId;

    @Getter
    @Setter
    @JsonProperty("is_auto_change_name")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private boolean isAutoChangeName = true;

    @Getter
    @Setter
    @JsonIgnore
    private boolean isCopy = false;

    @Override
    public MeasureRecommendationItem copy() {
        if (this.isCopy()) {
            return this;
        }
        val res = JsonUtil.deepCopyQuietly(this, MeasureRecommendationItem.class);
        res.setCopy(true);
        return res;
    }

    @Override
    public void translate(OptimizeContext context) {
        if (context.getDeletedMeasureRecommendations().contains(itemId)) {
            return;
        }
        new MeasureNameModifier(context).translate(itemId);
    }

    public void apply(OptimizeContext context, boolean real) {
        if (context.getDeletedMeasureRecommendations().contains(itemId)) {
            return;
        }
        var measure = context.getMeasureRecommendationItem(itemId).measure;
        Preconditions.checkNotNull(measure);

        if (real) {
            val oldId = measure.getId();
            val newId = context.getOriginMeasureIndex();
            measure.setId(newId);
            context.setOriginMeasureIndex(newId + 1);
            context.getTranslations().put(oldId, newId);
        }
        context.getVirtualMeasures().add(measure.getName());
        context.getVirtualMeasureIds().add(measure.getId());
        val model = context.getModel();
        model.getAllMeasures().add(measure);

    }

    @Override
    public void checkDependencies(OptimizeContext context, boolean real) {
        if (context.getDeletedMeasureRecommendations().contains(itemId)) {
            return;
        }
        var recommendation = context.getMeasureRecommendationItem(itemId);
        val measure = recommendation.getMeasure();
        for (int i = 0; i < measure.getFunction().getParameters().size(); i++) {
            var parameterDesc = measure.getFunction().getParameters().get(i);
            if (parameterDesc.isColumnType()) {
                val columns = context.getVirtualColumnIdMap();
                val value = context.getMeasureRecommendationItem(itemId).getMeasure().getFunction().getParameters()
                        .get(i).getValue().toUpperCase();
                if (!columns.containsKey(value) && !real) {
                    context.failMeasureRecommendationItem(itemId);
                    return;
                }
                if (!columns.containsKey(value) || real && OptimizeRecommendationManager.isVirtualColumnId(columns.get(value))) {
                    throw new DependencyLostException(String.format(
                            "measure lost dependency: column %s not exists in all columns, you may need pass it first",
                            value));
                }
            }
        }
    }

    private static class MeasureNameModifier extends NameModifier<MeasureRecommendationItem> {

        MeasureNameModifier(OptimizeContext context) {
            super(context);
        }

        @Override
        public MeasureRecommendationItem translate(long itemId) {
            var recommendation = context.getMeasureRecommendationItem(itemId);
            val measure = recommendation.getMeasure();
            for (int i = 0; i < measure.getFunction().getParameters().size(); i++) {
                var parameterDesc = measure.getFunction().getParameters().get(i);
                val res = visitExpr(parameterDesc.getValue());
                if (res.getSecond()) {
                    val copy = context.copyMeasureRecommendationItem(itemId);
                    copy.getMeasure().getFunction().getParameters().get(i)
                            .setValue(res.getFirst().toSqlString(HiveSqlDialect.DEFAULT).toString());
                }
            }
            return context.getMeasureRecommendationItem(itemId);
        }
    }
}
