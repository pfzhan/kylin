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

package io.kyligence.kap.metadata.recommendation.ref;

import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class DimensionRef extends RecommendationRef {

    public DimensionRef(int id) {
        this.setId(id);
    }

    public DimensionRef(RecommendationRef columnRef, int id, String dataType, boolean existed) {
        this.setId(id);
        this.setName(columnRef.getName());
        this.setContent(buildContent(columnRef.getName(), dataType));
        this.setDataType(dataType);
        this.setExisted(existed);
        this.setEntity(columnRef.getEntity());
        this.setExcluded(columnRef.isExcluded());
    }

    public void init() {
        if (getDependencies().isEmpty()) {
            return;
        }
        RecommendationRef dependRef = getDependencies().get(0);
        this.setName(dependRef.getName());
        this.setDataType(dependRef.getDataType());
        this.setContent(buildContent(getName(), getDataType()));
        this.setExisted(false);
        this.setEntity(dependRef);
    }

    private String buildContent(String columnName, String dataType) {
        Map<String, String> map = Maps.newHashMap();
        map.put("column", columnName);
        map.put("data_type", dataType);
        String content;
        try {
            content = JsonUtil.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
        return content;
    }

    @Override
    public void rebuild(String newName) {
        // do nothing at present
    }
}
