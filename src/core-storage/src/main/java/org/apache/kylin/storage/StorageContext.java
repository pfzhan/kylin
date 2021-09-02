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

package org.apache.kylin.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import io.kyligence.kap.metadata.cube.cuboid.NLayoutCandidate;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xjiang
 */
@Slf4j
public class StorageContext {
    @Getter
    private int ctxId;

    @Getter
    @Setter
    private Long layoutId = -1L;

    @Getter
    @Setter
    private Long streamingLayoutId = -1L;

    @Setter
    @Getter
    private boolean partialMatchModel = false;

    @Setter
    private NLayoutCandidate candidate;

    public NLayoutCandidate getCandidate() {
        if (isBatchCandidateEmpty() && !isStreamCandidateEmpty()) {
            return streamingCandidate;
        }
        return candidate == null ? NLayoutCandidate.EMPTY : candidate;
    }

    @Setter
    private NLayoutCandidate streamingCandidate;

    public NLayoutCandidate getStreamingCandidate() {
        return streamingCandidate == null ? NLayoutCandidate.EMPTY : streamingCandidate;
    }

    public boolean isBatchCandidateEmpty() {
        return candidate == null || candidate == NLayoutCandidate.EMPTY;
    }

    public boolean isStreamCandidateEmpty() {
        return streamingCandidate == null || streamingCandidate == NLayoutCandidate.EMPTY;
    }

    @Getter
    @Setter
    private Set<TblColRef> dimensions;

    @Getter
    @Setter
    private Set<FunctionDesc> metrics;

    @Getter
    @Setter
    private boolean useSnapshot = false;

    @Getter
    @Setter
    private List<NDataSegment> prunedSegments;

    @Getter
    @Setter
    private List<NDataSegment> prunedStreamingSegments;

    @Getter
    @Setter
    private Map<String, List<Long>> prunedPartitions;

    @Getter
    @Setter
    private boolean isEmptyLayout;

    public StorageContext(int ctxId) {
        this.ctxId = ctxId;
    }

}
