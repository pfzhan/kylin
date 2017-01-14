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

package io.kyligence.kap.modeling.auto.mockup;

import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.model.FunctionDesc;

import com.google.common.collect.Lists;

public class QueryResult {
    List<OLAPQueryResult> olapQueries = Lists.newArrayList();

    public List<OLAPQueryResult> getOLAPQueries() {
        return olapQueries;
    }

    public void addOLAPQuery(OLAPQueryResult olapQueryResult) {
        olapQueries.add(olapQueryResult);
    }

    @Override
    public String toString() {
        return "QueryResult{" + "olapQueries=" + olapQueries + '}';
    }

    public static class OLAPQueryResult {
        Set<Long> groupBy;
        Set<Long> filter;
        Set<FunctionDesc> measures;

        String cubeName;

        long cuboidId;

        public String getCubeName() {
            return cubeName;
        }

        public void setCubeName(String cubeName) {
            this.cubeName = cubeName;
        }

        public Set<Long> getGroupBy() {
            return groupBy;
        }

        public void setGroupBy(Set<Long> groupBy) {
            this.groupBy = groupBy;
        }

        public Set<Long> getFilter() {
            return filter;
        }

        public void setFilter(Set<Long> filter) {
            this.filter = filter;
        }

        public long getCuboidId() {
            return cuboidId;
        }

        public void setCuboidId(long cuboidId) {
            this.cuboidId = cuboidId;
        }

        public Set<FunctionDesc> getMeasures() {
            return measures;
        }

        public void setMeasures(Set<FunctionDesc> measures) {
            this.measures = measures;
        }

        @Override
        public String toString() {
            return "OLAPQueryResult{" + "groupBy=" + groupBy + ", filter=" + filter + ", cuboidId=" + cuboidId + '}';
        }
    }
}
