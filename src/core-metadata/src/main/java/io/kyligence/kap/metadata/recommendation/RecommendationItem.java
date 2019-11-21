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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlIdentifier;

import com.google.common.collect.ImmutableList;

import lombok.val;

public abstract class RecommendationItem<T extends RecommendationItem> {

    @Getter
    @Setter
    @JsonProperty("item_id")
    protected long itemId;

    @Getter
    @Setter
    @JsonProperty("create_time")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    protected long createTime;

    @Getter
    @Setter
    @JsonProperty("recommendation_type")
    protected RecommendationType recommendationType = RecommendationType.ADDITION;

    @Getter
    @Setter
    @JsonProperty("is_auto_change_name")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    protected boolean isAutoChangeName = true;

    @Getter
    @Setter
    @JsonIgnore
    protected boolean isCopy = false;

    abstract void checkDependencies(OptimizeContext context, boolean real);

    public void checkDependencies(OptimizeContext context) {
        checkDependencies(context, false);
    }

    abstract void apply(OptimizeContext context, boolean real);

    public void apply(OptimizeContext context) {
        apply(context, false);
    }

    abstract T copy();

    abstract static class NameModifier<T extends RecommendationItem<T>> extends OptimizeVisitor {
        protected OptimizeContext context;

        NameModifier(OptimizeContext context) {
            this.context = context;
        }

        public abstract T translate(long itemId);

        @Override
        public Boolean visit(SqlIdentifier identifier) {
            boolean modified = false;
            for (val pair : context.getNameTranslations()) {
                String oldName = pair.getFirst();
                String newName = pair.getSecond();
                if (identifier.names.size() == 2) {
                    final String[] values = identifier.names.toArray(new String[0]);
                    if (values[0].equals(context.getFactTableName()) && values[1].equals(oldName)) {
                        values[1] = newName;
                        identifier.names = ImmutableList.copyOf(values);
                        modified = true;
                    }

                }
            }
            return modified;
        }
    }

    abstract void translate(OptimizeContext context);
}
