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
public class RawTableColumnFamilyDesc {

    @JsonProperty("name")
    private String name;  
    @JsonProperty("column_refs")
    private String[] columnRefs;
    
    // Assembled at runtime: 
    private RawTableColumnDesc[] columns;
    private int[] columnIndex;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String[] getColumnRefs() {
        return columnRefs;
    }

    public void setColumnRefs(String[] columnRefs) {
        this.columnRefs = columnRefs;
    }
    
    public RawTableColumnDesc[] getColumns() {
        return columns;
    }
    
    public void setColumns(RawTableColumnDesc[] columns) {
        this.columns = columns;
    }
    
    public int[] getColumnIndex() {
        return columnIndex;
    }
    
    public void setColumnIndex(int[] columnIndex) {
        this.columnIndex = columnIndex;
    }
    
    public boolean containsColumn(String refName) {
        for (String ref : columnRefs) {
            if (ref.equals(refName))
                return true;
        }
        return false;
    }
       
    public int getColumnIndexByName(String refName) {
        
        for (int i = 0; i < columnIndex.length; i++) {
            if (columnRefs[i].equals(refName)) {
                return columnIndex[i];
            }
        }

        return -1;
    }
    
    @Override
    public String toString() {
        return "RawTableColumnFamilyDesc [name=" + name + ", columns=" + Arrays.toString(columns) + "]";
    }
    
}
