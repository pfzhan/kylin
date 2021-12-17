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
package io.kyligence.kap.secondstorage.metadata;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.secondstorage.metadata.annotation.TableDefinition;

@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE)
@TableDefinition
public class TableEntity implements Serializable, WithLayout {

    public static final int DEFAULT_SHARD = 100;

    public static final class Builder {

        private LayoutEntity layoutEntity;

        public Builder setLayoutEntity(LayoutEntity layoutEntity) {
            this.layoutEntity = layoutEntity;
            return this;
        }

        public TableEntity build() {
            TableEntity table = new TableEntity();
            table.layoutID = layoutEntity.getId();
            return table;
        }
    }
    public static Builder builder() {
        return new Builder();
    }

    @JsonBackReference
    private TablePlan tablePlan;

    @JsonProperty("layout_id")
    private long layoutID;

    @JsonProperty("shard_numbers")
    private int shardNumbers = DEFAULT_SHARD;

    public void checkIsNotCachedAndShared() {
        if (tablePlan != null)
            tablePlan.checkIsNotCachedAndShared();
    }

    public void setShardNumbers(int shardNumbers) {
        checkIsNotCachedAndShared();
        this.shardNumbers = shardNumbers;
    }

    public TablePlan getTablePlan() {
        return tablePlan;
    }

    public long getLayoutID() {
        return layoutID;
    }

    public int getShardNumbers() {
        return shardNumbers;
    }
}
