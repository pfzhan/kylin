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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.validation.constraints.Size;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.msg.MsgPicker;
import org.springframework.validation.FieldError;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.insensitive.ProjectInsensitiveRequest;
import io.kyligence.kap.rest.request.Validation;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

/**
 * if you're adding/removing fields from SQLRequest, take a look at getCacheKey
 */
@Getter
@Setter
@NoArgsConstructor
public class SQLRequest implements Serializable, ProjectInsensitiveRequest, Validation {
    protected static final long serialVersionUID = 1L;

    private String sql;
    private String queryId;
    private String project;
    private String username = "";
    private String executeAs;
    private Integer offset = 0;
    private Integer limit = 0;
    private boolean acceptPartial = false;
    private boolean forcedToPushDown = false;
    private long queryStartTime;
    private String stopId;

    private Map<String, String> backdoorToggles;

    @Size(max = 256)
    private String user_defined_tag;

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
        if (!Objects.equals(sql, that.sql))
            return false;
        if (!Objects.equals(project, that.project))
            return false;
        if (!Objects.equals(offset, that.offset))
            return false;
        if (!Objects.equals(limit, that.limit))
            return false;
        if (!Objects.equals(user_defined_tag, that.user_defined_tag))
            return false;
        return Objects.equals(backdoorToggles, that.backdoorToggles);

    }

    @Override
    public int hashCode() {
        int result = sql != null ? sql.hashCode() : 0;
        result = 31 * result + (project != null ? project.hashCode() : 0);
        result = 31 * result + (offset != null ? offset.hashCode() : 0);
        result = 31 * result + (limit != null ? limit.hashCode() : 0);
        result = 31 * result + (acceptPartial ? 1 : 0);
        result = 31 * result + (backdoorToggles != null ? backdoorToggles.hashCode() : 0);
        result = 31 * result + (user_defined_tag != null ? user_defined_tag.hashCode() : 0);
        return result;
    }

    @Override
    public String getErrorMessage(List<FieldError> errors) {
        val message = MsgPicker.getMsg();
        if (!CollectionUtils.isEmpty(errors) && errors.size() > 0) {
            if (errors.get(0).getField().equalsIgnoreCase("user_defined_tag")) {
                return message.getINVALID_USER_TAG();
            }
        }
        return "";
    }
}
