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

package io.kyligence.kap.ddl.exp;

/**
 * @author neng.liu
 */
public class ColumnWithAlias {
    private final boolean distinct;
    private final String alias;
    private final String name;

    private ColumnWithAlias(boolean distinct, String alias, String name) {
        this.distinct = distinct;
        this.alias = alias;
        this.name = name;
    }
    public static ColumnWithAliasBuilder builder() {
        return new ColumnWithAliasBuilder();
    }

    public static class ColumnWithAliasBuilder {

        private boolean distinct;
        private String alias;
        private String name;

        public ColumnWithAliasBuilder distinct(boolean distinct) {
            this.distinct = distinct;
            return this;
        }

        public ColumnWithAliasBuilder alias(String alias) {
            this.alias = alias;
            return this;
        }

        public ColumnWithAliasBuilder name(String name) {
            this.name = name;
            return this;
        }

        public ColumnWithAlias build() {
            return new ColumnWithAlias(distinct, alias, name);
        }
    }

    public boolean isDistinct() {
        return distinct;
    }

    public String getAlias() {
        return alias;
    }

    public String getName() {
        return name;
    }
}
