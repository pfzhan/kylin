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

package org.apache.kylin.query.routing.rules;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationFilter;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.RoutingRule;

import com.google.common.collect.Sets;

/**
 */
public class RemoveBlackoutRealizationsRule extends RoutingRule {
    private static Set<String> blackList = Sets.newHashSet();
    private static Set<String> whiteList = Sets.newHashSet();

    private static ConcurrentHashMap<KylinConfig, IRealizationFilter> filters = new ConcurrentHashMap<>();

    public static boolean accept(IRealization real) {
        String canonicalName = real.getCanonicalName();
        if (blackList.contains(canonicalName))
            return false;
        if (!whiteList.isEmpty() && !whiteList.contains(canonicalName))
            return false;

        String filterClz = real.getConfig().getQueryRealizationFilter();
        if (filterClz != null) {
            if (!getFilterImpl(real.getConfig()).accept(real))
                return false;
        }

        return true;
    }

    private static IRealizationFilter getFilterImpl(KylinConfig conf) {
        IRealizationFilter filter = filters.get(conf);
        if (filter == null) {
            try {
                Class<? extends IRealizationFilter> clz = ClassUtil.forName(conf.getQueryRealizationFilter(), IRealizationFilter.class);
                filter = clz.getConstructor(KylinConfig.class).newInstance(conf);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            filters.put(conf, filter);
        }
        return filter;
    }

    @Override
    public void apply(List<Candidate> candidates) {
        candidates.removeIf(candidate -> !accept(candidate.getRealization()));
    }

}
