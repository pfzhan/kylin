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

package io.kyligence.kap.metadata.favorite;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class FavoriteRule extends RootPersistentEntity {
    public static final String FREQUENCY_RULE_NAME = "frequency";
    public static final String COUNT_RULE_NAME = "count";
    public static final String DURATION_RULE_NAME = "duration";
    public static final String SUBMITTER_RULE_NAME = "submitter";
    public static final String SUBMITTER_GROUP_RULE_NAME = "submitter_group";
    public static final String REC_SELECT_RULE_NAME = "recommendations";
    public static final String BLACKLIST_NAME = "blacklist";
    public static final String EXCLUDED_TABLES_RULE = "excluded_tables";

    public FavoriteRule(List<AbstractCondition> conds, String name, boolean isEnabled) {
        this.conds = conds;
        this.name = name;
        this.enabled = isEnabled;
    }

    @JsonProperty("conds")
    private List<AbstractCondition> conds = Lists.newArrayList();
    @JsonProperty("name")
    private String name;
    @JsonProperty("enabled")
    private boolean enabled;

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
    @NoArgsConstructor
    public abstract static class AbstractCondition implements Serializable {

    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class Condition extends AbstractCondition {
        private String leftThreshold;
        private String rightThreshold;

        public Condition(String leftThreshold, String rightThreshold) {
            this.leftThreshold = leftThreshold;
            this.rightThreshold = rightThreshold;
        }
    }

    @Getter
    @Setter
    public static class SQLCondition extends AbstractCondition {
        private String id;
        @JsonProperty("sql_pattern")
        private String sqlPattern;
        @JsonProperty("create_time")
        private long createTime;

        public SQLCondition() {
            this.id = UUID.randomUUID().toString();
        }

        public SQLCondition(String sqlPattern) {
            this();
            this.sqlPattern = sqlPattern;
            this.createTime = System.currentTimeMillis();
        }

        @VisibleForTesting
        public SQLCondition(String id, String sqlPattern) {
            this.id = id;
            this.sqlPattern = sqlPattern;
            this.createTime = System.currentTimeMillis();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;

            if (this.getClass() != obj.getClass())
                return false;

            SQLCondition that = (SQLCondition) obj;
            return this.sqlPattern.equalsIgnoreCase(that.getSqlPattern());
        }

        @Override
        public int hashCode() {
            return this.sqlPattern.hashCode();
        }
    }

    public static FavoriteRule getDefaultRule(FavoriteRule rule, String name) {
        switch (name) {
        case COUNT_RULE_NAME:
        case SUBMITTER_GROUP_RULE_NAME:
        case SUBMITTER_RULE_NAME:
        case REC_SELECT_RULE_NAME:
            return rule == null ? new FavoriteRule(Lists.newArrayList(getDefaultCondition(name)), name, true) : rule;
        case FREQUENCY_RULE_NAME:
        case DURATION_RULE_NAME:
        case EXCLUDED_TABLES_RULE:
            return rule == null ? new FavoriteRule(Lists.newArrayList(getDefaultCondition(name)), name, false) : rule;
        default:
            return rule;
        }
    }

    public static Condition getDefaultCondition(String ruleName) {
        switch (ruleName) {
        case COUNT_RULE_NAME:
            return new Condition(null, "10");
        case FREQUENCY_RULE_NAME:
            return new Condition(null, "0.1");
        case SUBMITTER_RULE_NAME:
            return new Condition(null, "ADMIN");
        case SUBMITTER_GROUP_RULE_NAME:
            return new Condition(null, "ROLE_ADMIN");
        case DURATION_RULE_NAME:
            return new Condition("0", "180");
        case REC_SELECT_RULE_NAME:
            return new Condition(null, "20");
        case EXCLUDED_TABLES_RULE:
            return new Condition(null, "");
        default:
            return null;
        }
    }
}