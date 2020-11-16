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

package io.kyligence.kap.metadata.acl;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import io.kyligence.kap.common.obf.IKeep;
import org.apache.kylin.metadata.datatype.DataType;

import java.util.Set;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SensitiveDataMask implements IKeep {

    private static final Set<String> VALID_DATA_TYPES = Sets.newHashSet(
        DataType.STRING, DataType.VARCHAR, DataType.CHAR,
        DataType.INT, DataType.INTEGER, DataType.BIGINT, DataType.SMALL_INT, DataType.TINY_INT,
        DataType.DOUBLE, DataType.FLOAT, DataType.REAL, DataType.DECIMAL,
        DataType.DATE, DataType.TIMESTAMP, DataType.DATETIME
    );

    public static boolean isValidDataType(String dataType) {
        int parenthesesIdx = dataType.indexOf('(');
        return VALID_DATA_TYPES.contains(parenthesesIdx > -1 ?
                dataType.substring(0, parenthesesIdx).trim().toLowerCase() : dataType.trim().toLowerCase());
    }

    public enum MaskType implements IKeep {
        DEFAULT(0),  // mask sensitive data by type with default values
        AS_NULL(1);  // mask all sensitive data as NULL

        int priority = 0; // smaller number stands for higher priority

        MaskType(int priority) {
            this.priority = priority;
        }

        public MaskType merge(MaskType other) {
            if (other == null) {
                return this;
            }
            return this.priority < other.priority ? this : other;
        }
    }

    @JsonProperty
    String column;

    @JsonProperty
    MaskType type;

    public SensitiveDataMask() {
    }

    public SensitiveDataMask(String column, MaskType type) {
        this.column = column;
        this.type = type;
    }

    public MaskType getType() {
        return type;
    }

    public String getColumn() {
        return column;
    }
}
