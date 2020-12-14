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
package io.kyligence.kap.metadata.model.schema;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.val;

public enum SchemaNodeType {
    TABLE_COLUMN {
        @Override
        public String getSubject(String key) {
            return key.substring(0, key.lastIndexOf('.'));
        }

        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return key.substring(key.lastIndexOf('.') + 1);
        }
    },
    MODEL_COLUMN {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    },
    MODEL_CC, //
    MODEL_TABLE {
        @Override
        public String getSubject(String key) {
            return key;
        }

        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return key;
        }
    },
    MODEL_FACT, MODEL_DIM, //
    MODEL_PARTITION(true) {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("column");
        }
    },
    MODEL_MULTIPLE_PARTITION(true) {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return String.join(",", ((List<String>) attributes.get("columns")));
        }
    },
    MODEL_JOIN(true), MODEL_FILTER(true) {
        @Override
        public String getSubject(String key) {
            return key;
        }

        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("condition");
        }
    }, //
    MODEL_DIMENSION {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    },
    MODEL_MEASURE {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    }, //
    WHITE_LIST_INDEX {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    },
    TO_BE_DELETED_INDEX {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    },
    RULE_BASED_INDEX {
        @Override
        public String getDetail(String key, Map<String, Object> attributes) {
            return (String) attributes.get("id");
        }
    }, //
    AGG_GROUP, INDEX_AGG_SHARD, INDEX_AGG_EXTEND_PARTITION;

    @Getter
    boolean causeModelBroken;

    SchemaNodeType() {
        this(false);
    }

    SchemaNodeType(boolean causeModelBroken) {
        this.causeModelBroken = causeModelBroken;
    }

    public SchemaNode withKey(String key) {
        return new SchemaNode(this, key);
    }

    public boolean isModelNode() {
        return this != TABLE_COLUMN && this != MODEL_TABLE;
    }

    public String getSubject(String key) {
        return key.split("/")[0];
    }

    public String getDetail(String key, Map<String, Object> attributes) {
        val words = key.split("/");
        if (words.length == 2) {
            return words[1];
        }
        return this.toString();
    }
}
