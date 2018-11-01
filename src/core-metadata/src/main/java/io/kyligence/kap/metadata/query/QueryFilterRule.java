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

package io.kyligence.kap.metadata.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.kylin.common.persistence.RootPersistentEntity;

import java.io.Serializable;
import java.util.List;

public class QueryFilterRule extends RootPersistentEntity {
    public static final String FREQUENCY = "frequency";
    public static final String DURATION = "duration";
    public static final String SUBMITTER = "submitter";

    public static final String FREQUENCY_RULE_NAME = "frequency";
    public static final String DURATION_RULE_NAME = "duration";
    public static final String SUBMITTER_RULE_NAME = "submitter";

    public static final String ENABLE = "enable";

    public QueryFilterRule() {
        updateRandomUuid();
        this.conds = Lists.newArrayList();
    }

    public QueryFilterRule(List<QueryHistoryCond> conds, String name, boolean isEnabled) {
        updateRandomUuid();
        this.conds = conds;
        this.name = name;
        this.enabled = isEnabled;
    }

    @JsonProperty("conds")
    private List<QueryHistoryCond> conds;
    @JsonProperty("name")
    private String name;
    @JsonProperty("enabled")
    private boolean enabled;

    public List<QueryHistoryCond> getConds() {
        return conds;
    }

    public void setConds(List<QueryHistoryCond> conds) {
        this.conds = conds;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public static class QueryHistoryCond implements Serializable {
        private String field;
        private String rightThreshold;
        private String leftThreshold;

        public QueryHistoryCond() {

        }

        public QueryHistoryCond(String field, String leftThreshold, String rightThreshold) {
            this.field = field;
            this.leftThreshold = leftThreshold;
            this.rightThreshold = rightThreshold;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public String getRightThreshold() {
            return rightThreshold;
        }

        public void setRightThreshold(String rightThreshold) {
            this.rightThreshold = rightThreshold;
        }

        public String getLeftThreshold() {
            return leftThreshold;
        }

        public void setLeftThreshold(String leftThreshold) {
            this.leftThreshold = leftThreshold;
        }
    }
}