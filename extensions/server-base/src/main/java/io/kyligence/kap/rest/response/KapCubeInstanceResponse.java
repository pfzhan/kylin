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

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.rest.response.CubeInstanceResponse;

import com.fasterxml.jackson.annotation.JsonProperty;

public class KapCubeInstanceResponse extends CubeInstanceResponse {
    @JsonProperty("cube_size")
    private Float cubeSize;
    @JsonProperty("source_records")
    private long sourceRecords;
    @JsonProperty("last_build_time")
    private long lastBuildTime;
    @JsonProperty("input_records_size")
    private long inputRecordsSize;

    public KapCubeInstanceResponse(CubeInstance cubeInstance) {
        super(cubeInstance);
        setUuid(cubeInstance.getUuid());
        setLastModified(cubeInstance.getLastModified());
        setVersion(cubeInstance.getVersion());
        setName(cubeInstance.getName());
        setOwner(cubeInstance.getOwner());
        setDescName(cubeInstance.getDescName());
        setCost(cubeInstance.getCost());
        setStatus(cubeInstance.getStatus());
        setSegments(cubeInstance.getSegments());
        setCreateTimeUTC(cubeInstance.getCreateTimeUTC());

    }

    public void setCubeSize(Float cubeSize) {
        this.cubeSize = cubeSize;
    }

    public void setSourceRecords(long sourceRecords) {
        this.sourceRecords = sourceRecords;
    }

    public void setLastBuildTime(long lastBuildTime) {
        this.lastBuildTime = lastBuildTime;
    }

    public void setInputRecordsSize(long inputRecordsSize) {
        this.inputRecordsSize = inputRecordsSize;
    }
}
