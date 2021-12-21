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

package com.google.common.cache;

import java.util.Locale;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;

import com.google.common.base.Equivalence;

import lombok.experimental.UtilityClass;

@UtilityClass
public class CustomKeyEquivalenceCacheBuilder {

    private static final Equivalence<Object> KEY_CASE_IGNORE_EQUIVALENCE = new Equivalence<Object>() {
        @Override
        protected boolean doEquivalent(Object a, Object b) {
            if (a instanceof String && b instanceof String) {
                return Objects.equals(((String) a).toLowerCase(Locale.ROOT), ((String) b).toLowerCase(Locale.ROOT));
            }
            return Objects.equals(a, b);
        }

        @Override
        protected int doHash(Object o) {
            if (o instanceof String) {
                return ((String) o).toLowerCase(Locale.ROOT).hashCode();
            }
            return o.hashCode();
        }
    };

    public static CacheBuilder<Object, Object> newBuilder() {
        if (KylinConfig.getInstanceFromEnv().isMetadataKeyCaseInSensitiveEnabled()) {
            return CacheBuilder.newBuilder().keyEquivalence(KEY_CASE_IGNORE_EQUIVALENCE);
        }
        return CacheBuilder.newBuilder().keyEquivalence(Equivalence.equals());
    }

}
