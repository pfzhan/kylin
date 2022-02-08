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
package io.kyligence.kap.rest.config;

import org.springframework.util.ClassUtils;

import io.kyligence.kap.guava20.shaded.common.base.Function;
import io.kyligence.kap.guava20.shaded.common.base.Optional;
import io.kyligence.kap.guava20.shaded.common.base.Predicate;
import springfox.documentation.RequestHandler;

public class KylinRequestHandlerSelectors {
    private KylinRequestHandlerSelectors() {
    }

    private static Optional<? extends Class<?>> declaringClass(RequestHandler input) {
        return Optional.fromNullable(input.declaringClass());
    }

    public static Predicate<RequestHandler> baseCurrentPackage(final String basePackage) {
        return input -> (Boolean) KylinRequestHandlerSelectors.declaringClass(input)
                .transform(KylinRequestHandlerSelectors.handlerPackage(basePackage)).or(false);
    }

    private static Function<Class<?>, Boolean> handlerPackage(final String basePackage) {
        return input -> ClassUtils.getPackageName(input).startsWith(basePackage)
                && 1 == ClassUtils.getPackageName(input).substring(basePackage.length()).split(".").length;
    }

}
