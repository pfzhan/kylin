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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.Set;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DependentColumn {

    @JsonProperty
    String column;

    @JsonProperty("dependent_column_identity")
    String dependentColumnIdentity;

    @JsonProperty("dependent_values")
    String[] dependentValues;

    public DependentColumn() {
    }

    public DependentColumn(String column, String dependentColumnIdentity, String[] dependentValues) {
        this.column = column;
        this.dependentColumnIdentity = dependentColumnIdentity;
        this.dependentValues = dependentValues;
    }

    public String getColumn() {
        return column;
    }

    public String getDependentColumnIdentity() {
        return dependentColumnIdentity;
    }

    public String[] getDependentValues() {
        return dependentValues;
    }

    public DependentColumn merge(DependentColumn other) {
        Preconditions.checkArgument(other != null);
        Preconditions.checkArgument(other.column.equalsIgnoreCase(this.column));
        Preconditions.checkArgument(other.dependentColumnIdentity.equalsIgnoreCase(this.dependentColumnIdentity));
        Set<String> values = Sets.newHashSet(dependentValues);
        values.addAll(Arrays.asList(other.dependentValues));
        return new DependentColumn(column, dependentColumnIdentity, values.toArray(new String[0]));
    }
}
