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

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

import com.google.common.collect.Lists;

import io.kyligence.kap.junit.annotation.MultiTimezoneTest;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;

public class MultiTimezoneProvider implements TestTemplateInvocationContextProvider {

    private static final String METHOD_CONTEXT_KEY = "context";

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        if (!context.getTestMethod().isPresent()) {
            return false;
        }

        Method testMethod = context.getTestMethod().get();
        val annotations = findAnnotation(context.getElement(), MultiTimezoneTest.class);
        if (!annotations.isPresent()) {
            return false;
        }
        val methodContext = new MultiTimezoneContext(testMethod, annotations.get().timezones());
        getStore(context).put(METHOD_CONTEXT_KEY, methodContext);
        return true;

    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        return getStore(context).get(METHOD_CONTEXT_KEY, MultiTimezoneContext.class).getTimezones().stream()
                .map(x -> new TimezoneInvocationContext(x));
    }

    private ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getStore(
                ExtensionContext.Namespace.create(MultiTimezoneProvider.class, context.getRequiredTestMethod()));
    }

    @Data
    static class MultiTimezoneContext {
        private Method testMethod;
        private List<String> timezones;

        public MultiTimezoneContext(Method testMethod, String[] timezones) {
            this.testMethod = testMethod;
            this.timezones = Lists.newArrayList(timezones);
        }
    }

    @AllArgsConstructor
    static class TimezoneInvocationContext implements TestTemplateInvocationContext {

        private String timezone;

        @Override
        public String getDisplayName(int invocationIndex) {
            return "[" + timezone + "]";
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Collections.singletonList(new TimezoneExtension(timezone));
        }
    }

}
