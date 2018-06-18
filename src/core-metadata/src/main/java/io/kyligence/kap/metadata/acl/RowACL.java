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

package io.kyligence.kap.metadata.acl;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.acl.ColumnToConds.Cond;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE //
        , getterVisibility = JsonAutoDetect.Visibility.NONE //
        , isGetterVisibility = JsonAutoDetect.Visibility.NONE //
        , setterVisibility = JsonAutoDetect.Visibility.NONE) //
public class RowACL extends RootPersistentEntity implements IKeep {
    @JsonProperty("path")
    private String path;

    //all row conds in the table, for example:C1:{cond1, cond2},C2{cond1, cond3}, immutable
    @JsonProperty("rowACL")
    private ColumnToConds columnToConds = new ColumnToConds();

    RowACL() {
    }

    public RowACL(Map<String, List<Cond>> columnToConds) {
        this.columnToConds.putAll(columnToConds);
    }

    @Override
    public String resourceName() {
        return this.path;
    }

    public void init(String project, String type, String user, String table) {
        this.path = project + "/" + type + "/" + user + "/" + table;
    }

    public int size() {
        return columnToConds.size();
    }

    public ColumnToConds getColumnToConds() {
        return columnToConds;
    }

    public void add(ColumnToConds columnToConds) {
        this.columnToConds = columnToConds;
    }

}