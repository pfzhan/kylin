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
package io.kyligence.kap.rest.response;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;
import org.apache.kylin.metadata.model.TableDesc;

import java.util.Set;

@Getter
@Setter
public class SnapshotResponse implements Comparable<SnapshotResponse> {

    @JsonProperty("table")
    private String table;

    @JsonProperty("database")
    private String database;

    @JsonProperty("usage")
    private int usage;

    @JsonProperty("storage")
    private long storage;

    @JsonProperty("fact_table_count")
    private int factTableCount;

    @JsonProperty("lookup_table_count")
    private int lookupTableCount;

    @JsonProperty("last_modified_time")
    private long lastModifiedTime;

    @JsonProperty("status")
    private String status;

    @JsonProperty("forbidden_colunms")
    private Set<String> columns;

    public SnapshotResponse() {}

    public SnapshotResponse(TableDesc tableDesc, long storage, int factTableCount, int lookupTableCount,
                            long lastModifiedTime, String status, Set<String> columns) {


        this.table = tableDesc.getName();
        this.database = tableDesc.getDatabase();
        this.usage = tableDesc.getSnapshotHitCount();
        this.storage = storage;
        this.factTableCount = factTableCount;
        this.lookupTableCount = lookupTableCount;
        this.lastModifiedTime = lastModifiedTime;
        this.status = status;
        this.columns = columns;
    }

    @Override
    public int compareTo(SnapshotResponse o) {
        if (this.lastModifiedTime == 0) {
            return -1;
        }

        if (o.lastModifiedTime == 0) {
            return 1;
        }

        int nonNegative = o.lastModifiedTime > this.lastModifiedTime ? 1 : 0;
        return o.lastModifiedTime < this.lastModifiedTime ? -1 : nonNegative;
    }
}
