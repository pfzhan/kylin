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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.Setter;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ParameterDesc implements Serializable {

    public static ParameterDesc newInstance(Object... objs) {
        if (objs.length == 0)
            throw new IllegalArgumentException();

        ParameterDesc r = new ParameterDesc();

        Object obj = objs[0];
        if (obj instanceof TblColRef) {
            TblColRef col = (TblColRef) obj;
            r.type = FunctionDesc.PARAMETER_TYPE_COLUMN;
            r.value = col.getIdentity();
            r.colRef = col;
        } else {
            r.type = FunctionDesc.PARAMETER_TYPE_CONSTANT;
            r.value = (String) obj;
        }

        if (objs.length >= 2) {
            r.nextParameter = newInstance(Arrays.copyOfRange(objs, 1, objs.length));
        }
        return r;
    }

    @Getter
    @Setter
    @JsonProperty("type")
    private String type;
    @Getter
    @Setter
    @JsonProperty("value")
    private String value;

    @Getter
    @Setter
    @JsonProperty("next_parameter")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private ParameterDesc nextParameter;

    @Getter
    private TblColRef colRef = null;
    private transient List<TblColRef> allColRefsIncludingNexts = null;
    private transient List<PlainParameter> plainParameters = null;

    // Lazy evaluation
    public List<PlainParameter> getPlainParameters() {
        if (plainParameters == null) {
            plainParameters = PlainParameter.createFromParameterDesc(this);
        }
        return plainParameters;
    }

    public byte[] getBytes() throws UnsupportedEncodingException {
        return value.getBytes("UTF-8");
    }

    void setColRef(TblColRef colRef) {
        this.colRef = colRef;
        this.allColRefsIncludingNexts = null;
    }

    public List<TblColRef> getColRefs() {
        if (allColRefsIncludingNexts == null) {
            List<TblColRef> all = new ArrayList<>(2);
            ParameterDesc p = this;
            while (p != null) {
                if (p.isColumnType())
                    all.add(p.getColRef());

                p = p.nextParameter;
            }
            allColRefsIncludingNexts = all;
        }
        return allColRefsIncludingNexts;
    }

    public boolean isColumnType() {
        return FunctionDesc.PARAMETER_TYPE_COLUMN.equals(type);
    }

    public boolean isConstant() {
        return FunctionDesc.PARAMETER_TYPE_CONSTANT.equalsIgnoreCase(type);
    }

    public boolean isConstantParameterDesc() {
        TblColRef colRef = this.getColRef();
        if (colRef == null || isConstant())
            return true;
        Set<TblColRef> collector = Sets.newHashSet();
        TblColRef.collectSourceColumns(colRef, collector);
        return collector.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ParameterDesc that = (ParameterDesc) o;

        if (type != null ? !type.equals(that.type) : that.type != null)
            return false;

        ParameterDesc p = this;
        ParameterDesc q = that;
        for (; p != null && q != null; p = p.nextParameter, q = q.nextParameter) {
            if (p.isColumnType() != q.isColumnType()) {
                return false;
            }

            if (p.isColumnType() && !Objects.equals(q.getColRef(), p.getColRef())) {
                return false;
            }

            if (!p.isColumnType() && !p.value.equals(q.value)) {
                return false;
            }
        }

        return p == q;
    }

    public boolean equalInArbitraryOrder(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ParameterDesc that = (ParameterDesc) o;

        List<PlainParameter> thisPlainParams = this.getPlainParameters();
        List<PlainParameter> thatPlainParams = that.getPlainParameters();

        return thisPlainParams.containsAll(thatPlainParams) && thatPlainParams.containsAll(thisPlainParams);
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (colRef != null ? colRef.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        String thisStr = isColumnType() && colRef != null ? colRef.toString() : value;
        return nextParameter == null ? thisStr : thisStr + "," + nextParameter.toString();
    }

    /**
     * PlainParameter is created to present ParameterDesc in List style.
     * Compared to ParameterDesc its advantage is:
     * 1. easy to compare without considering order
     * 2. easy to compare one by one
     */
    @Setter
    @Getter
    public static class PlainParameter implements Serializable {
        private String type;
        private String value;
        private TblColRef colRef = null;

        private PlainParameter() {
        }

        public boolean isColumnType() {
            return FunctionDesc.PARAMETER_TYPE_COLUMN.equals(type);
        }

        static List<PlainParameter> createFromParameterDesc(ParameterDesc parameterDesc) {
            List<PlainParameter> result = new ArrayList<>();
            ParameterDesc local = parameterDesc;
            while (local != null) {
                if (local.isColumnType()) {
                    result.add(createSingleColumnParameter(local));
                } else {
                    result.add(createSingleValueParameter(local));
                }
                local = local.nextParameter;
            }
            return result;
        }

        static PlainParameter createSingleValueParameter(ParameterDesc parameterDesc) {
            PlainParameter single = new PlainParameter();
            single.type = parameterDesc.type;
            single.value = parameterDesc.value;
            return single;
        }

        static PlainParameter createSingleColumnParameter(ParameterDesc parameterDesc) {
            PlainParameter single = new PlainParameter();
            single.type = parameterDesc.type;
            single.value = parameterDesc.value;
            single.colRef = parameterDesc.colRef;
            return single;
        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (colRef != null ? colRef.hashCode() : 0);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            PlainParameter that = (PlainParameter) o;

            if (type != null ? !type.equals(that.type) : that.type != null)
                return false;

            if (this.isColumnType()) {
                if (!that.isColumnType())
                    return false;
                // check for ParameterDesc create from json
                if (this.colRef == null && that.colRef == null)
                    return Objects.equals(this.value, that.value);
                // check for normal case
                return Objects.equals(this.colRef, that.colRef);
            } else {
                if (that.isColumnType())
                    return false;
                return this.value.equals(that.value);
            }

        }
    }
}
