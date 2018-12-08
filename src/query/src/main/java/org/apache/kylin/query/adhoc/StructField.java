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
package org.apache.kylin.query.adhoc;

import lombok.NoArgsConstructor;

public class StructField {
    private String name;
    private int dataType;
    private String dataTypeName;
    private int precision;
    private int scale;
    private boolean nullable;

    private StructField(String name, int dataType, String dataTypeName, int precision, int scale, boolean nullable) {
        this.name = name;
        this.dataType = dataType;
        this.dataTypeName = dataTypeName;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public int getDataType() {
        return dataType;
    }

    public String getDataTypeName() {
        return dataTypeName;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public boolean isNullable() {
        return nullable;
    }

    @NoArgsConstructor
    static public class StructFieldBuilder {
        private String name;
        private int dataType;
        private String dataTypeName;
        private int precision;
        private int scale;
        private boolean nullable;

        public StructFieldBuilder setName(String name) {
            this.name = name;
            return this;
        }

        public StructFieldBuilder setDataType(int dataType) {
            this.dataType = dataType;
            return this;
        }

        public StructFieldBuilder setDataTypeName(String dataTypeName) {
            this.dataTypeName = dataTypeName;
            return this;
        }

        public StructFieldBuilder setPrecision(int precision) {
            this.precision = precision;
            return this;
        }

        public StructFieldBuilder setScale(int scale) {
            this.scale = scale;
            return this;
        }

        public StructFieldBuilder setNullable(boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public StructField createStructField() {
            return new StructField(name, dataType, dataTypeName, precision, scale, nullable);
        }
    }

}
