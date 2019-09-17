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

package io.kyligence.kap.common.logging;

import org.apache.commons.lang3.ClassUtils;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLoggerFactory;
import org.slf4j.helpers.Util;

import java.util.ArrayList;
import java.util.List;

public final class QueryLoggerFactory implements ILoggerFactory {

    static final ILoggerFactory innerLoggerFactory;

    private static String[] innerLoggerFactoryNames = new String[] { "org.apache.logging.slf4j.Log4jLoggerFactory",
            "org.slf4j.impl.Log4jLoggerFactory" };

    static {
        final List<ILoggerFactory> factories = new ArrayList<>(innerLoggerFactoryNames.length);
        for (final String loggerFactoryName : innerLoggerFactoryNames) {
            final Class<?> loggerFactoryClass = getIfPresent(loggerFactoryName);
            if (loggerFactoryClass != null) {
                try {
                    final ILoggerFactory loggerFactory = (ILoggerFactory) loggerFactoryClass.newInstance();
                    factories.add(loggerFactory);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }

            }
        }

        if (factories.isEmpty()) {
            Util.report("Can not find internal SLF4J ILoggerFactory instance, used NOPLoggerFactory.");
            innerLoggerFactory = new NOPLoggerFactory();
        } else {
            if (factories.size() > 1) {
                Util.report("Class path contains multiple internal SLF4J ILoggerFactory instance.");
                for (final ILoggerFactory factory : factories) {
                    Util.report("Found internal ILoggerFactory [" + factory.getClass().getName() + "]");
                }
            }

            innerLoggerFactory = factories.get(0);
            Util.report("Used " + innerLoggerFactory.getClass().getName());
        }
    }

    private static Class<?> getIfPresent(String className) {
        try {
            return ClassUtils.getClass(className);
        } catch (Throwable ex) {
            return null;
        }
    }

    @Override
    public Logger getLogger(String name) {
        return new QueryLoggerAdapter(innerLoggerFactory.getLogger(name));
    }

}
