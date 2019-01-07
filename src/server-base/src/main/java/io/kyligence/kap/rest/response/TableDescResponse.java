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
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Setter
@Getter
public class TableDescResponse extends TableDesc {
    @JsonProperty("exd")
    Map<String, String> descExd = new HashMap<String, String>();
    @JsonProperty("root_fact")
    boolean rootFact;
    @JsonProperty("lookup")
    boolean lookup;
    @JsonProperty("cardinality")
    Map<String, Long> cardinality = new HashMap<String, Long>();
    @JsonProperty("primary_key")
    Set<String> primaryKey = new HashSet<>();
    @JsonProperty("foreign_key")
    Set<String> foreignKey = new HashSet<>();
    @JsonProperty("partitioned_column")
    private String partitionedColumn;
    @JsonProperty("segment_range")
    private SegmentRange segmentRange;
    @JsonProperty("storage_size")
    private long storageSize = -1;
    @JsonProperty("total_records")
    private long totalRecords;

    public TableDescResponse(TableDesc table) {
        super(table);
    }


}