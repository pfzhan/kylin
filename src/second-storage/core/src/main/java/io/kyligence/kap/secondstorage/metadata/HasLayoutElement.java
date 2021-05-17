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
package io.kyligence.kap.secondstorage.metadata;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;

import java.util.List;
import java.util.Optional;

public interface HasLayoutElement<E extends WithLayout> {

    // utility
    static <E extends WithLayout> boolean sameLayout(E e, LayoutEntity layoutEntity){
        return e.getLayoutID() == layoutEntity.getId();
    }

    static <E extends WithLayout> boolean sameIndex(E e, LayoutEntity layoutEntity){
        return layoutEntity.getIndexId() == e.getLayoutID() / 10000 * 10000;
    }

    List<E> all();
    default Optional<E> getEntity(LayoutEntity layoutEntity, boolean sameLayout){
        return all().stream()
                .filter(e -> sameLayout ? sameLayout(e, layoutEntity): sameIndex(e, layoutEntity))
                .findFirst();
    }
    default Optional<E> getEntity(LayoutEntity layoutEntity) {
        return getEntity(layoutEntity, true);
    }

    default boolean containIndex(LayoutEntity layoutEntity, boolean throwOnDifferentLayout){
        return getEntity(layoutEntity, false)
                .map(e -> {
                            if (!throwOnDifferentLayout || sameLayout(e, layoutEntity))
                                return true;
                            else
                                throw new IllegalStateException("");
                        })
                .orElse(false);
    }
}
