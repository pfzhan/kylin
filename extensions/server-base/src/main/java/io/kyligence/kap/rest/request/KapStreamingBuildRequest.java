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

package io.kyligence.kap.rest.request;

import java.util.Map;

import com.google.common.collect.Maps;

public class KapStreamingBuildRequest {
    
    private String mpValues;
    
    private long sourceOffsetStart;

    private long sourceOffsetEnd;

    private Map<Integer, Long> sourcePartitionOffsetStart = Maps.newHashMap();

    private Map<Integer, Long> sourcePartitionOffsetEnd = Maps.newHashMap();

    private String buildType;

    private boolean force;

    public String getMpValues() {
        return mpValues;
    }

    public void setMpValues(String mpValues) {
        this.mpValues = mpValues;
    }

    public long getSourceOffsetStart() {
        return sourceOffsetStart;
    }

    public void setSourceOffsetStart(long sourceOffsetStart) {
        this.sourceOffsetStart = sourceOffsetStart;
    }

    public long getSourceOffsetEnd() {
        return sourceOffsetEnd;
    }

    public void setSourceOffsetEnd(long sourceOffsetEnd) {
        this.sourceOffsetEnd = sourceOffsetEnd;
    }

    public Map<Integer, Long> getSourcePartitionOffsetStart() {
        return sourcePartitionOffsetStart;
    }

    public void setSourcePartitionOffsetStart(Map<Integer, Long> sourcePartitionOffsetStart) {
        this.sourcePartitionOffsetStart = sourcePartitionOffsetStart;
    }

    public Map<Integer, Long> getSourcePartitionOffsetEnd() {
        return sourcePartitionOffsetEnd;
    }

    public void setSourcePartitionOffsetEnd(Map<Integer, Long> sourcePartitionOffsetEnd) {
        this.sourcePartitionOffsetEnd = sourcePartitionOffsetEnd;
    }

    public String getBuildType() {
        return buildType;
    }

    public void setBuildType(String buildType) {
        this.buildType = buildType;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

}
