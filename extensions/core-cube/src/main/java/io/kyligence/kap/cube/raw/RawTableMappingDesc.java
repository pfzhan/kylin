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

package io.kyligence.kap.cube.raw;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class RawTableMappingDesc implements java.io.Serializable {

    @JsonProperty("column_family")
    private RawTableColumnFamilyDesc[] columnFamily;

    // point to the raw table instance which contain this RawTableMappingDesc instance.
    private RawTableDesc rawTableRef;

    public RawTableDesc getRawTableRef() {
        return rawTableRef;
    }

    public void setRawTableRef(RawTableDesc rawTableRef) {
        this.rawTableRef = rawTableRef;
    }

    public RawTableColumnFamilyDesc[] getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(RawTableColumnFamilyDesc[] columnFamily) {
        this.columnFamily = columnFamily;
    }

    public void init(RawTableDesc rawTableDesc) {
        rawTableRef = rawTableDesc;

        for (RawTableColumnFamilyDesc cf : columnFamily) {
            cf.setName(cf.getName().toUpperCase());
        }
    }

    public void initAsSeparatedColumns(RawTableDesc rawTableDesc) {
        this.rawTableRef = rawTableDesc;

        // In raw table with no column family, columns are ordered in sortby-columns-first manner. 
        int cfNum = rawTableDesc.getColumnDescsInOrder().size();        
        columnFamily = new RawTableColumnFamilyDesc[cfNum];

        for (int i = 0; i < cfNum; i++) {
            RawTableColumnFamilyDesc cf = new RawTableColumnFamilyDesc();   
            RawTableColumnDesc col = rawTableDesc.getColumnDescsInOrder().get(i);
            String columnRef = col.getName();
            cf.setColumnRefs(new String[] { columnRef });
            cf.setName("F" + (i + 1));
            columnFamily[i] = cf;
        }
    }

    @Override
    public String toString() {
        return "HBaseMappingDesc [columnFamily=" + Arrays.toString(columnFamily) + "]";
    }
    
}
