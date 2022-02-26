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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Lists;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter(AccessLevel.PROTECTED)
@EqualsAndHashCode
@NoArgsConstructor
@ToString
public abstract class RecommendationRef {

    // id >= 0 column in model
    // id < 0 column in rawRecItem
    private int id;
    private String name;
    private String content;
    @ToString.Exclude
    private String dataType;
    private boolean isBroken;
    private boolean existed;
    private boolean crossModel;
    private boolean isExcluded;
    private Object entity;
    private List<RecommendationRef> dependencies = Lists.newArrayList();

    public abstract void rebuild(String newName);

    protected <T extends RecommendationRef> List<RecommendationRef> validate(List<T> refs) {
        if (CollectionUtils.isEmpty(refs)) {
            return Lists.newArrayList();
        }
        return refs.stream().filter(ref -> !ref.isBroken() && !ref.isExisted()).collect(Collectors.toList());
    }

    // When a ref is not deleted and does not exist in model.
    public boolean isEffective() {
        return !this.isBroken() && !this.isExisted() && this.getId() < 0;
    }

    // When a ref derives from origin model or is effective.
    public boolean isLegal() {
        return !this.isBroken() && (this.getId() >= 0 || !this.isExisted());
    }
}
