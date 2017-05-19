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

package io.kyligence.kap.modeling.smart.cube;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.modeling.smart.query.QueryStats;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)

public class CubeLog extends RootPersistentEntity {
    private static final Logger logger = LoggerFactory.getLogger(CubeLog.class);

    @JsonProperty("cube_name")
    private String cubeName;

    @JsonProperty("sample_sqls")
    private List<String> sampleSqls = new ArrayList<>();

    @JsonProperty("query_stats")
    private QueryStats queryStats;

    public CubeLog() {
    }

    public void setQueryStats(QueryStats queryStats) {
        this.queryStats = queryStats;
    }

    public QueryStats getQueryStats() {
        return this.queryStats;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public String getCubeName() {
        return this.cubeName;
    }

    public void setSampleSqls(List<String> sampleSqls) {
        this.sampleSqls = sampleSqls;
    }

    public List<String> getSampleSqls() {
        return this.sampleSqls;
    }

    public String getResourcePath() {
        return CubeLogManager.CUBE_LOG_STATISTICS_ROOT + "/" + cubeName + MetadataConstants.FILE_SURFIX;
    }
}
