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
package org.apache.kylin.rest.util;

import java.util.List;

import io.kyligence.kap.metadata.query.NativeQueryRealization;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.response.SQLResponse;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import lombok.val;

public class QueryCacheSignatureUtil {

    public static String createCacheSignature(SQLResponse response, String project) {
        List<String> signature = Lists.newArrayList();
        val realizations = response.getNativeRealizations();
        Preconditions.checkState(CollectionUtils.isNotEmpty(realizations));
        for (int i = 0; i < realizations.size(); i++) {
            signature.add(generateSignature(realizations.get(i), project));
        }
        return Joiner.on(",").join(signature);
    }

    public static boolean checkCacheExpired(SQLResponse sqlResponse, String project) {
        val signature = sqlResponse.getSignature();
        if (StringUtils.isBlank(signature)) {
            return false;
        }
        val lastSignature = createCacheSignature(sqlResponse, project);
        return !signature.equals(lastSignature);
    }

    private static String generateSignature(NativeQueryRealization realization, String project) {
        val modelId = realization.getModelId();
        Preconditions.checkState(StringUtils.isNotBlank(modelId));
        val dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId);
        return dataflow == null ? "" : String.valueOf(dataflow.getLastModified());
    }
}
