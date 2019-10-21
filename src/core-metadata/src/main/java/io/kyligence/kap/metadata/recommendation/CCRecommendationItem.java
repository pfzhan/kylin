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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.kylin.common.util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.var;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class CCRecommendationItem implements Serializable, RecommendationItem<CCRecommendationItem> {

    @Getter
    @Setter
    @JsonProperty("item_id")
    private long itemId;

    @Getter
    @Setter
    @JsonProperty("cc_column_id")
    private int ccColumnId;

    @Getter
    @Setter
    @JsonProperty("cc")
    private ComputedColumnDesc cc;

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
    @JsonIgnore
    private boolean isCopy = false;

    public void apply(OptimizeContext context, boolean real) {
        if (context.getDeletedCCRecommendations().contains(itemId)) {
            return;
        }
        var item = context.getCCRecommendationItem(itemId);
        val cc = item.cc;
        Preconditions.checkNotNull(cc);

        val model = context.getModel();
        cc.init(model, model.getRootFactTable().getAlias());

        val ccs = model.getComputedColumnDescs();
        ccs.add(cc);

        val columnInModel = new NDataModel.NamedColumn();
        columnInModel.setId(getCcColumnId());
        columnInModel.setName(context.getFactTableName() + "_" + cc.getColumnName());
        columnInModel.setAliasDotColumn(context.getFactTableName() + "." + cc.getColumnName());
        columnInModel.setStatus(NDataModel.ColumnStatus.EXIST);
        model.getAllNamedColumns().add(columnInModel);
        if (real) {
            int realId = context.getOriginColumnIndex();
            context.getTranslations().put(columnInModel.getId(), realId);
            columnInModel.setId(realId);
            context.setOriginColumnIndex(realId + 1);
        }
        context.getVirtualColumnIdMap().put(columnInModel.getAliasDotColumn(), columnInModel.getId());
        context.getVirtualIdColumnMap().put(columnInModel.getId(), columnInModel);
        context.getAllCCNames().add(cc.getColumnName());
    }

    public void checkDependencies(OptimizeContext context, boolean real) {
        if (context.getDeletedCCRecommendations().contains(itemId)) {
            return;
        }
        new CheckDependenciesVisitor(context, real).visit();
    }

    @AllArgsConstructor
    private class CheckDependenciesVisitor extends OptimizeVisitor {
        private OptimizeContext context;
        private boolean real;

        @Override
        public Boolean visit(SqlIdentifier identifier) {
            String table = context.getFactTableName();
            String name = identifier.names.get(0);
            if (identifier.names.size() == 2) {
                table = identifier.names.get(0);
                name = identifier.names.get(1);

            }
            String aliasDotColumn = (table + "." + name).toUpperCase();
            val columnIdMap = context.getVirtualColumnIdMap();
            if (!columnIdMap.containsKey(aliasDotColumn) && !real) {
                context.failCCRecommendationItem(itemId);
                return true;
            }
            if (!columnIdMap.containsKey(aliasDotColumn) || real
                    && OptimizeRecommendationManager.isVirtualColumnId(columnIdMap.get(aliasDotColumn))) {
                throw new DependencyLostException(String
                        .format("cc lost dependency: cc %s not exists, you may need pass it first", aliasDotColumn));
            }
            return false;
        }

        public void visit() {
            visitExpr(context.getCCRecommendationItem(itemId).getCc().getExpression());
        }
    }

    public CCRecommendationItem copy() {
        if (this.isCopy()) {
            return this;
        }
        val res = JsonUtil.deepCopyQuietly(this, CCRecommendationItem.class);
        res.setCopy(true);
        return res;
    }

    public void translate(OptimizeContext context) {
        if (context.getDeletedCCRecommendations().contains(itemId)) {
            return;
        }
        new CCNameModifier(context).translate(itemId);
    }

    private static class CCNameModifier extends NameModifier<CCRecommendationItem> {

        CCNameModifier(OptimizeContext context) {
            super(context);
        }

        @Override
        public CCRecommendationItem translate(long itemId) {
            val recommendation = context.getCCRecommendationItem(itemId);
            val res = visitExpr(recommendation.getCc().getExpression());
            if (res.getSecond()) {
                val copy = context.copyCCRecommendationItem(itemId);
                copy.getCc().setExpression(res.getFirst().toSqlString(HiveSqlDialect.DEFAULT).toString());
                return copy;
            }
            return recommendation;
        }
    }
}
