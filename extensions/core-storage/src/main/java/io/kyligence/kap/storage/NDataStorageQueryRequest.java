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

package io.kyligence.kap.storage;

import java.io.Serializable;
import java.util.Set;

import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;

import io.kyligence.kap.cube.model.NCuboidLayout;

@SuppressWarnings("serial")
public class NDataStorageQueryRequest  implements Serializable {
    private NCuboidLayout cuboidLayout;
    private Set<TblColRef> dimensions;
    private Set<TblColRef> groups;
    private Set<TblColRef> filterCols;
    private Set<FunctionDesc> metrics;
    private TupleFilter filter;
    private TupleFilter havingFilter;
    private StorageContext context;

    public NDataStorageQueryRequest(NCuboidLayout cuboidLayout, Set<TblColRef> dimensions, Set<TblColRef> groups, //
            Set<TblColRef> filterCols, Set<FunctionDesc> metrics, TupleFilter filter, TupleFilter havingFilter, StorageContext context) {
        this.setCuboidLayout(cuboidLayout);
        this.dimensions = dimensions;
        this.groups = groups;
        this.filterCols = filterCols;
        this.metrics = metrics;
        this.filter = filter;
        this.havingFilter = havingFilter;
        this.context = context;
    }

    public NCuboidLayout getCuboidLayout() {
        return cuboidLayout;
    }

    public void setCuboidLayout(NCuboidLayout cuboidLayout) {
        this.cuboidLayout = cuboidLayout;
    }

    public Set<TblColRef> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Set<TblColRef> dimensions) {
        this.dimensions = dimensions;
    }

    public Set<TblColRef> getGroups() {
        return groups;
    }

    public void setGroups(Set<TblColRef> groups) {
        this.groups = groups;
    }

    public Set<FunctionDesc> getMetrics() {
        return metrics;
    }

    public void setMetrics(Set<FunctionDesc> metrics) {
        this.metrics = metrics;
    }

    public TupleFilter getFilter() {
        return filter;
    }

    public void setFilter(TupleFilter filter) {
        this.filter = filter;
    }

    public TupleFilter getHavingFilter() {
        return havingFilter;
    }

    public void setHavingFilter(TupleFilter havingFilter) {
        this.havingFilter = havingFilter;
    }

    public StorageContext getContext() {
        return context;
    }

    public void setContext(StorageContext context) {
        this.context = context;
    }

    public Set<TblColRef> getFilterCols() {
        return filterCols;
    }

    public void setFilterCols(Set<TblColRef> filterCols) {
        this.filterCols = filterCols;
    }
}
