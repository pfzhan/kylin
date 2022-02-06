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
package io.kyligence.kap.junit;

import java.util.Map;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.junit.annotation.OverwriteProp;
import lombok.val;

public class OverwritePropExtension implements BeforeEachCallback, AfterEachCallback, BeforeAllCallback {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace
            .create(OverwritePropExtension.class);
    private static final String OVERWRITE_PROP_BEFORE_ALL_KEY = "OverwriteProp_all";
    private static final String OVERWRITE_PROP_BEFORE_EACH_KEY = "OverwriteProp_each";
    private static final String OVERWRITTEN_PROP_KEY = "OverwrittenProp";

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        readFromAnnotation(context, OVERWRITE_PROP_BEFORE_ALL_KEY);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        readFromAnnotation(context, OVERWRITE_PROP_BEFORE_EACH_KEY);
        Map<String, String> overwritten = context.getStore(NAMESPACE).getOrComputeIfAbsent(OVERWRITTEN_PROP_KEY,
                __ -> Maps.newHashMap(), Map.class);
        Map<String, String> props = context.getStore(NAMESPACE).getOrDefault(OVERWRITE_PROP_BEFORE_ALL_KEY, Map.class,
                Maps.newHashMap());
        props.forEach((k, v) -> Unsafe.overwriteSystemProp(overwritten, k, v));
        props = context.getStore(NAMESPACE).getOrDefault(OVERWRITE_PROP_BEFORE_EACH_KEY, Map.class, Maps.newHashMap());
        props.forEach((k, v) -> Unsafe.overwriteSystemProp(overwritten, k, v));
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        Map<String, String> overwritten = context.getStore(NAMESPACE).getOrComputeIfAbsent(OVERWRITTEN_PROP_KEY,
                __ -> Maps.newHashMap(), Map.class);
        val keys = Sets.newHashSet(overwritten.keySet());
        for (String property : keys) {
            if (!overwritten.containsKey(property) || overwritten.get(property) == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, overwritten.get(property));
            }
        }
        context.getStore(NAMESPACE).remove(OVERWRITE_PROP_BEFORE_EACH_KEY);
        context.getStore(NAMESPACE).remove(OVERWRITTEN_PROP_KEY);
    }

    private void readFromAnnotation(ExtensionContext context, String key) {
        AnnotationSupport.findRepeatableAnnotations(context.getElement(), OverwriteProp.class)
                .forEach(x -> context.getStore(NAMESPACE).getOrComputeIfAbsent(key, __ -> Maps.newHashMap(), Map.class)
                        .put(x.key(), x.value()));
    }

    /** Clear system property in test method with annotation {@link org.junit.Test} */
    public final void restoreSystemProp(Map<String, String> overwritten, String property) {
        if (!overwritten.containsKey(property) || overwritten.get(property) == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, overwritten.get(property));
        }
        overwritten.remove(property);
    }
}
