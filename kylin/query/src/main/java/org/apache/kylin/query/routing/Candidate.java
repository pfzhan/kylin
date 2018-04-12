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

package org.apache.kylin.query.routing;

import java.util.Collections;
import java.util.Map;

import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;

import com.google.common.collect.Maps;

public class Candidate implements Comparable<Candidate> {

    static Map<String, Integer> DEFAULT_PRIORITIES = Maps.newHashMap();
    static Map<String, Integer> PRIORITIES = DEFAULT_PRIORITIES;

    static {
        DEFAULT_PRIORITIES.put("INVERTED_INDEX", 1);
        DEFAULT_PRIORITIES.put("NCUBE", 1);
    }

    /** for test only */
    public static void setPriorities(Map<String, Integer> priorities) {
        PRIORITIES = Collections.unmodifiableMap(priorities);
    }

    /** for test only */
    public static void restorePriorities() {
        PRIORITIES = Collections.unmodifiableMap(DEFAULT_PRIORITIES);
    }

    // ============================================================================

    IRealization realization;
    SQLDigest sqlDigest;
    int priority;
    CapabilityResult capability;

    public Candidate(IRealization realization, SQLDigest sqlDigest) {
        this.realization = realization;
        this.sqlDigest = sqlDigest;
        this.priority = PRIORITIES.get(realization.getType());
    }

    public IRealization getRealization() {
        return realization;
    }

    public SQLDigest getSqlDigest() {
        return sqlDigest;
    }

    public int getPriority() {
        return priority;
    }

    public CapabilityResult getCapability() {
        return capability;
    }

    public void setCapability(CapabilityResult capability) {
        this.capability = capability;
    }

    @Override
    public int compareTo(Candidate o) {
        int comp = this.priority - o.priority;
        if (comp != 0) {
            return comp;
        }

        comp = this.capability.cost - o.capability.cost;
        if (comp != 0) {
            return comp;
        }

        return 0;
    }

    @Override
    public String toString() {
        return realization.toString();
    }
}
