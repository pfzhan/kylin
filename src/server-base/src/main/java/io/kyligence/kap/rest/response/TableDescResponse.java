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
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
    @JsonProperty("segment_ranges")
    private Map<SegmentRange, SegmentStatusEnum> segmentRanges = new HashMap<>();
    @JsonProperty("water_mark_start")
    private int waterMarkStart = -1;
    @JsonProperty("water_mark_end")
    private int waterMarkEnd = -1;
    @JsonProperty("start_time")
    private long startTime = -1;
    @JsonProperty("end_time")
    private long endTime = -1;

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public int getWaterMarkStart() {
        return waterMarkStart;
    }

    public void setWaterMarkStart(int waterMarkStart) {
        this.waterMarkStart = waterMarkStart;
    }

    public int getWaterMarkEnd() {
        return waterMarkEnd;
    }

    public void setWaterMarkEnd(int waterMarkEnd) {
        this.waterMarkEnd = waterMarkEnd;
    }

    public Map<SegmentRange, SegmentStatusEnum> getSegmentRanges() {
        return segmentRanges;
    }

    public void setSegmentRanges(Map<SegmentRange, SegmentStatusEnum> segmentRanges) {
        this.segmentRanges = segmentRanges;
    }

    public String getPartitionedColumn() {
        return partitionedColumn;
    }

    public void setPartitionedColumn(String partitionedColumn) {
        this.partitionedColumn = partitionedColumn;
    }

    public Set<String> getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(Set<String> primaryKey) {
        this.primaryKey = primaryKey;
    }

    public Set<String> getForeignKey() {
        return foreignKey;
    }

    public void setForeignKey(Set<String> foreignKey) {
        this.foreignKey = foreignKey;
    }

    public boolean isRootFact() {
        return rootFact;
    }

    public void setRootFact(boolean rootFact) {
        this.rootFact = rootFact;
    }

    public boolean isLookup() {
        return lookup;
    }

    public void setLookup(boolean lookup) {
        this.lookup = lookup;
    }

    /**
     * @return the cardinality
     */
    public Map<String, Long> getCardinality() {
        return cardinality;
    }

    /**
     * @param cardinality
     *            the cardinality to set
     */
    public void setCardinality(Map<String, Long> cardinality) {
        this.cardinality = cardinality;
    }

    /**
     * @return the descExd
     */
    public Map<String, String> getDescExd() {
        return descExd;
    }

    /**
     * @param descExd
     *            the descExd to set
     */
    public void setDescExd(Map<String, String> descExd) {
        this.descExd = descExd;
    }

    /**
     * @param table
     */
    public TableDescResponse(TableDesc table) {
        super(table);
    }

}