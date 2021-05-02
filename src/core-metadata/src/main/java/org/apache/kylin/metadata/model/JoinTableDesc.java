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
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.model.NDataModel;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class JoinTableDesc implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String FLATTEN = "flatten";
    public static final String NORMALIZED = "normalized";

    @JsonProperty("table")
    private String table;

    @JsonProperty("kind")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private NDataModel.TableKind kind = NDataModel.TableKind.LOOKUP;

    @JsonProperty("alias")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String alias;

    @JsonProperty("join")
    private JoinDesc join;

    @JsonProperty("flattenable")
    private String flattenable;

    @JsonProperty("join_relation_type")
    private ModelJoinRelationTypeEnum joinRelationTypeEnum = ModelJoinRelationTypeEnum.MANY_TO_ONE;

    private TableRef tableRef;

    public boolean isFlattenable() {
        return this.flattenable == null || FLATTEN.equalsIgnoreCase(this.flattenable);
    }

    public boolean isDerivedForbidden() {
        return isFlattenable() && isToManyJoinRelation();
    }

    private boolean isToManyJoinRelation() {
        return this.joinRelationTypeEnum == ModelJoinRelationTypeEnum.MANY_TO_MANY
                || this.joinRelationTypeEnum == ModelJoinRelationTypeEnum.ONE_TO_MANY;
    }

    public boolean isDerivedToManyJoinRelation() {
        return !isFlattenable() && isToManyJoinRelation();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        JoinTableDesc that = (JoinTableDesc) o;

        if (table != null ? !table.equals(that.table) : that.table != null)
            return false;
        if (kind != that.kind)
            return false;
        if (alias != null ? !alias.equals(that.alias) : that.alias != null)
            return false;
        if (!Objects.equals(this.joinRelationTypeEnum, that.joinRelationTypeEnum)) {
            return false;
        }
        return join != null ? join.equals(that.join) : that.join == null;
    }

    @Override
    public int hashCode() {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (kind != null ? kind.hashCode() : 0);
        result = 31 * result + (alias != null ? alias.hashCode() : 0);
        result = 31 * result + (join != null ? join.hashCode() : 0);
        result = 31 * result + Objects.hashCode(joinRelationTypeEnum);
        return result;
    }
}
