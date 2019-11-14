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

import org.apache.calcite.sql.SqlIdentifier;

import com.google.common.collect.ImmutableList;

import lombok.val;

public interface RecommendationItem<T extends RecommendationItem> {
    long getItemId();

    void setItemId(long itemId);

    void checkDependencies(OptimizeContext context, boolean real);

    default void checkDependencies(OptimizeContext context) {
        checkDependencies(context, false);
    }

    void apply(OptimizeContext context, boolean real);

    default void apply(OptimizeContext context) {
        apply(context, false);
    }

    T copy();

    abstract class NameModifier<T extends RecommendationItem<T>> extends OptimizeVisitor {
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

    void translate(OptimizeContext context);
}
