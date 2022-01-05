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
package io.kyligence.kap.streaming.request;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import lombok.Data;
import org.apache.kylin.metadata.model.SegmentRange;

import java.io.Serializable;
import java.util.List;

@Data
public class StreamingSegmentRequest implements Serializable, IKeep {
    private String project;

    private String dataflowId;

    private SegmentRange segmentRange;

    private String newSegId;

    private List<NDataSegment> removeSegment;

    private String layer;

    private String status;

    private Long sourceCount = -1L;

    public StreamingSegmentRequest() {

    }

    public StreamingSegmentRequest(String project, String dataflowId) {
        this.project = project;
        this.dataflowId = dataflowId;
    }

    public StreamingSegmentRequest(String project, String dataflowId, Long sourceCount) {
        this.project = project;
        this.dataflowId = dataflowId;
        this.sourceCount = sourceCount;
    }
}