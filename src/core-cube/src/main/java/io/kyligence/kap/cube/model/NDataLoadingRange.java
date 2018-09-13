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


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.cube.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.SegmentRange;

import java.util.List;

import static org.apache.kylin.common.persistence.ResourceStore.DATA_LOADING_RANGE_RESOURCE_ROOT;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataLoadingRange extends RootPersistentEntity {

    public NDataLoadingRange() {}

    private String project;
    @JsonProperty("table_name")
    private String tableName;
    @JsonProperty("column_name")
    private String columnName;
    @JsonProperty("segment_ranges")
    private List<SegmentRange> segmentRanges = Lists.newArrayList();
    // (waterMarkStart, waterMarkEnd]
    @JsonProperty("water_mark_start")
    private int waterMarkStart = -1;
    @JsonProperty("water_mark_end")
    private int waterMarkEnd = -1;

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public List<SegmentRange> getSegmentRanges() {
        return segmentRanges;
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

    public SegmentRange getCoveredSegmentRange(){
        SegmentRange readySegmentRange = null;

        if (CollectionUtils.isEmpty(segmentRanges)) {
            return readySegmentRange;
        }

        SegmentRange start = segmentRanges.get(0);
        if (segmentRanges.size() == 1) {
            return start;
        }

        SegmentRange end = segmentRanges.get(segmentRanges.size() - 1);
        readySegmentRange = start.coverWith(end);

        return readySegmentRange;
    }

    public SegmentRange getCoveredReadySegmentRange(){
        SegmentRange readySegmentRange = null;

        if (CollectionUtils.isEmpty(segmentRanges) || (waterMarkEnd == waterMarkStart)) {
            return readySegmentRange;
        }

        SegmentRange end = segmentRanges.get(waterMarkEnd);
        if (segmentRanges.size() == 1) {
            return end;
        }

        SegmentRange start = segmentRanges.get(waterMarkStart + 1);
        readySegmentRange = start.coverWith(end);

        return readySegmentRange;
    }

    @Override
    public String resourceName() {
        return tableName;
    }

    @Override
    public String getResourcePath() {
        return new StringBuilder().append("/").append(project).append(DATA_LOADING_RANGE_RESOURCE_ROOT).append("/")
                .append(resourceName()).append(MetadataConstants.FILE_SURFIX).toString();
    }
}
