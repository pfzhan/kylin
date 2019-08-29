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

package org.apache.kylin.rest.request;

import java.io.Serializable;
import java.util.Map;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * if you're adding/removing fields from SQLRequest, take a look at getCacheKey
 */
@Getter
@Setter
@NoArgsConstructor
public class SQLRequest implements Serializable {
    protected static final long serialVersionUID = 1L;

    private String sql;
    private String queryId;

    private String project;
    private String username = "";
    private Integer offset = 0;
    private Integer limit = 0;
    private boolean acceptPartial = false;

    private Map<String, String> backdoorToggles;

    protected volatile Object cacheKey = null;

    public Object getCacheKey() {
        if (cacheKey != null)
            return cacheKey;

        cacheKey = Lists.newArrayList(sql.replaceAll("[ ]", " ") //
                , project //
                , offset //
                , limit //
                , acceptPartial //
                , backdoorToggles //
                , username);
        return cacheKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        SQLRequest that = (SQLRequest) o;

        if (acceptPartial != that.acceptPartial)
            return false;
        if (sql != null ? !sql.equals(that.sql) : that.sql != null)
            return false;
        if (project != null ? !project.equals(that.project) : that.project != null)
            return false;
        if (offset != null ? !offset.equals(that.offset) : that.offset != null)
            return false;
        if (limit != null ? !limit.equals(that.limit) : that.limit != null)
            return false;
        return backdoorToggles != null ? backdoorToggles.equals(that.backdoorToggles) : that.backdoorToggles == null;

    }

    @Override
    public int hashCode() {
        int result = sql != null ? sql.hashCode() : 0;
        result = 31 * result + (project != null ? project.hashCode() : 0);
        result = 31 * result + (offset != null ? offset.hashCode() : 0);
        result = 31 * result + (limit != null ? limit.hashCode() : 0);
        result = 31 * result + (acceptPartial ? 1 : 0);
        result = 31 * result + (backdoorToggles != null ? backdoorToggles.hashCode() : 0);
        return result;
    }
}
