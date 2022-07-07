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

package io.kyligence.kap.common.metrics;

import java.util.Map;

public class MetricsObject {

    private boolean initStatus;
    private String fieldName;
    private String category;
    private String entity;
    private Map<String, String> tags;

    public MetricsObject(String fieldName, String category, String entity, Map<String, String> tags) {
        this.fieldName = fieldName;
        this.category = category;
        this.entity = entity;
        this.tags = tags;
        this.initStatus = false;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(fieldName);
        builder.append(",");
        builder.append(category);
        builder.append(",");
        builder.append(entity);
        builder.append(",");
        builder.append(tags == null ? "" : tags.toString());
        return builder.toString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getCategory() {
        return category;
    }

    public String getEntity() {
        return entity;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public boolean isInitStatus() {
        return initStatus;
    }

    public void setInitStatus(boolean initStatus) {
        this.initStatus = initStatus;
    }
}
