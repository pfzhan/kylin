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

package org.apache.kylin.gridtable;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

public class GTScanRange {

    final public GTRecord pkStart; // inclusive, record must not be null, col[pk].array() can be null to mean unbounded
    final public GTRecord pkEnd; // inclusive, record must not be null, col[pk].array() can be null to mean unbounded
    final public List<GTRecord> fuzzyKeys; // partial matching primary keys

    public GTScanRange(GTRecord pkStart, GTRecord pkEnd) {
        this(pkStart, pkEnd, null);
    }

    public GTScanRange(GTRecord pkStart, GTRecord pkEnd, List<GTRecord> fuzzyKeys) {
        GTInfo info = pkStart.info;
        assert info == pkEnd.info;

        this.pkStart = pkStart;
        this.pkEnd = pkEnd;
        this.fuzzyKeys = fuzzyKeys == null ? Collections.<GTRecord> emptyList() : fuzzyKeys;
    }

    public GTScanRange replaceGTInfo(final GTInfo gtInfo) {
        List<GTRecord> newFuzzyKeys = Lists.newArrayList();
        for (GTRecord input : fuzzyKeys) {
            newFuzzyKeys.add(new GTRecord(gtInfo, input.cols));
        }
        return new GTScanRange(new GTRecord(gtInfo, pkStart.cols), //
                new GTRecord(gtInfo, pkEnd.cols), //
                newFuzzyKeys);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((fuzzyKeys == null) ? 0 : fuzzyKeys.hashCode());
        result = prime * result + ((pkEnd == null) ? 0 : pkEnd.hashCode());
        result = prime * result + ((pkStart == null) ? 0 : pkStart.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        GTScanRange other = (GTScanRange) obj;
        if (fuzzyKeys == null) {
            if (other.fuzzyKeys != null)
                return false;
        } else if (!fuzzyKeys.equals(other.fuzzyKeys))
            return false;
        if (pkEnd == null) {
            if (other.pkEnd != null)
                return false;
        } else if (!pkEnd.equals(other.pkEnd))
            return false;
        if (pkStart == null) {
            if (other.pkStart != null)
                return false;
        } else if (!pkStart.equals(other.pkStart))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return (pkStart == null ? "null" : pkStart.toString(pkStart.info.primaryKey)) //
                + "-" + (pkEnd == null ? "null" : pkEnd.toString(pkEnd.info.primaryKey));
    }
}
