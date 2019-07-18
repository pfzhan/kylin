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

package org.apache.kylin.rest.response;

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.query.NativeQueryRealization;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SQLResponse implements Serializable {
    protected static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(SQLResponse.class);

    // the data type for each column
    protected List<SelectedColumnMeta> columnMetas;

    // the results rows, each row contains several columns
    protected List<List<String>> results;

    // if not select query, only return affected row count
    protected int affectedRowCount;

    // flag indicating whether an exception occurred
    @JsonProperty("isException")
    protected boolean isException;

    // if isException, the detailed exception message
    protected String exceptionMessage;

    // if isException, the related Exception
    protected Throwable throwable;

    protected long duration;

    protected boolean isPartial = false;

    protected List<Long> scanRows;

    @JsonProperty("totalScanRows")
    public long getTotalScanRows() {
        if (scanRows == null) {
            return -1;
        } else {
            return scanRows.stream().reduce((x, y) -> x + y).orElse(-1L);
        }
    }

    protected List<Long> scanBytes;

    @JsonProperty("totalScanBytes")
    public long getTotalScanBytes() {
        if (scanBytes == null) {
            return -1;
        } else {
            return scanBytes.stream().reduce((x, y) -> x + y).orElse(-1L);
        }
    }

    protected long resultRowCount;

    protected int shufflePartitions;

    protected boolean hitExceptionCache = false;

    protected boolean storageCacheUsed = false;

    @JsonProperty("pushDown")
    protected boolean queryPushDown = false;

    private boolean isPrepare = false;

    private boolean isTimeout;

    protected byte[] queryStatistics;

    protected String traceUrl = null;

    protected String queryId;

    private String server;

    private String suite;

    @Setter
    @Getter
    private String signature;

    @JsonProperty("realizations")
    private List<NativeQueryRealization> nativeRealizations;

    private String engineType;

    public SQLResponse() {
    }

    public SQLResponse(List<SelectedColumnMeta> columnMetas, List<List<String>> results, int affectedRowCount,
            boolean isException, String exceptionMessage) {
        this.columnMetas = columnMetas;
        this.results = results;
        this.affectedRowCount = affectedRowCount;
        this.isException = isException;
        this.exceptionMessage = exceptionMessage;
        if(results != null){
            this.resultRowCount = results.size();
        }
    }

    public SQLResponse(List<SelectedColumnMeta> columnMetas, List<List<String>> results,
            int affectedRowCount, boolean isException, String exceptionMessage, boolean isPartial, boolean isPushDown) {
        this.columnMetas = columnMetas;
        this.results = results;
        this.affectedRowCount = affectedRowCount;
        this.isException = isException;
        this.exceptionMessage = exceptionMessage;
        this.isPartial = isPartial;
        this.queryPushDown = isPushDown;
        this.isPrepare = BackdoorToggles.getPrepareOnly();
        if(results != null){
            this.resultRowCount = results.size();
        }
    }

    @JsonIgnore
    public Throwable getThrowable() {
        return throwable;
    }
}
