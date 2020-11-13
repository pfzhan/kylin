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

package io.kyligence.kap.smart.common;

import java.io.Serializable;
import java.util.Set;

import lombok.NoArgsConstructor;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class AccelerateInfo implements Serializable {

    private Set<QueryLayoutRelation> relatedLayouts = Sets.newHashSet();
    private Throwable failedCause;
    private String pendingMsg;

    public boolean isFailed() {
        return this.failedCause != null;
    }

    public boolean isPending() {
        return StringUtils.isNotBlank(pendingMsg);
    }

    public boolean isNotSucceed() {
        return isPending() || isFailed();
    }

    @Getter
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    public static class QueryLayoutRelation implements Serializable {

        @ToString.Exclude
        private String sql;
        @Setter
        private String modelId;
        @Setter
        private long layoutId;
        private int semanticVersion;

        public boolean consistent(LayoutEntity layout) {

            Preconditions.checkNotNull(layout);
            return this.semanticVersion == layout.getModel().getSemanticVersion()
                    && this.modelId.equalsIgnoreCase(layout.getModel().getId()) && this.layoutId == layout.getId();
        }
    }
}
