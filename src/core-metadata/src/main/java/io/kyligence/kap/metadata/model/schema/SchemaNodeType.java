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

import lombok.Getter;

public enum SchemaNodeType {
    TABLE_COLUMN, MODEL_COLUMN, MODEL_CC, //
    MODEL_PARTITION(true), MODEL_JOIN(true), MODEL_FILTER(true), //
    MODEL_DIMENSION, MODEL_MEASURE, //
    WHITE_LIST_INDEX, TO_BE_DELETED_INDEX, RULE_BASED_INDEX, //
    AGG_GROUP, INDEX_AGG_SHARD, INDEX_AGG_EXTEND_PARTITION, //
    ;

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
        return this != TABLE_COLUMN;
    }
}
