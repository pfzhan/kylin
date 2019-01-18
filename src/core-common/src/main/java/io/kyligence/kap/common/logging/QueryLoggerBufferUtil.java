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

import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.SetThreadName;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class QueryLoggerBufferUtil {

    private static final String[] QUERY_LOGGER_NAME_PATTERNS = new String[] {
            "org.apache.kylin.rest.service.QueryService", "io.kyligence.kap.query", "org.apache.kylin.query",
            "io.kyligence.kap.storage", "org.apache.kylin.storage" };

    // thread & subthread binding
    private static final InheritableThreadLocal<LinkedBlockingQueue<BufferedMessage>> QUERY_LOGGER_BUFFER = new InheritableThreadLocal<LinkedBlockingQueue<BufferedMessage>>();

    // Query Id -> log message buffer
    private static final ConcurrentMap<String, LinkedBlockingQueue<BufferedMessage>> QUERY_CONTEXT = new ConcurrentHashMap<>();

    private QueryLoggerBufferUtil() {
    }

    public static boolean buffer(String queryId, String level, String msg) {
        final LinkedBlockingQueue<BufferedMessage> buffer = QUERY_CONTEXT.get(queryId);
        if (buffer != null) {
            return buffer.offer(new BufferedMessage(level, msg));
        }

        return false;
    }

    public static boolean buffer(String queryId, String level, String format, Object... arguments) {
        final LinkedBlockingQueue<BufferedMessage> buffer = QUERY_CONTEXT.get(queryId);
        if (buffer != null) {
            return buffer.offer(new BufferedMessage(level, format, arguments));
        }

        return false;
    }

    public static boolean bufferIfNecessary(String loggerName, String level, String msg) {
        if (isMark() && isQueryLogger(loggerName)) {
            return QUERY_LOGGER_BUFFER.get().offer(new BufferedMessage(level, msg));
        }

        return false;
    }

    public static boolean bufferIfNecessary(String loggerName, String level, String format, Object... arguments) {
        if (isMark() && isQueryLogger(loggerName)) {
            return QUERY_LOGGER_BUFFER.get().offer(new BufferedMessage(level, format, arguments));
        }

        return false;
    }

    public static boolean bufferIfNecessary(String loggerName, String level, String msg, Throwable t) {
        if (isMark() && isQueryLogger(loggerName)) {
            return QUERY_LOGGER_BUFFER.get().offer(new BufferedMessage(level, msg, t));
        }

        return false;
    }

    public static void dump(String queryId) {
        dump(queryId, QueryLoggerFactory.innerLoggerFactory.getLogger(QueryLoggerBufferUtil.class.getName()));
    }

    public static void dump(String queryId, Logger output) {
        try {
            final Collection<BufferedMessage> messages = QUERY_CONTEXT.get(queryId);
            if (messages == null || messages.isEmpty()) {
                return;
            }

            try (SetThreadName ignored = new SetThreadName("Query %s", queryId)) {
                for (final BufferedMessage message : messages) {
                    message.flush(output);
                }
            } catch (IOException ex) {
                output.error("dump query [{}] log message failed", queryId, ex);
            }

        } finally {
            clear(queryId);
        }
    }

    public static void mark(String queryId) {
        final LinkedBlockingQueue<BufferedMessage> buffer = new LinkedBlockingQueue<>();
        QUERY_LOGGER_BUFFER.set(buffer);
        QUERY_CONTEXT.putIfAbsent(queryId, buffer);
    }

    public static boolean isMark() {
        return QUERY_LOGGER_BUFFER.get() != null;
    }

    public static void clear() {
        QUERY_LOGGER_BUFFER.remove();
        QUERY_CONTEXT.remove(QueryContext.current().getQueryId());
    }

    public static void clear(String queryId) {
        QUERY_LOGGER_BUFFER.remove();
        QUERY_CONTEXT.remove(queryId);
    }

    public static boolean isQueryLogger(String loggerName) {
        for (final String queryLoggerNamePattern : QUERY_LOGGER_NAME_PATTERNS) {
            if (loggerName.equals(queryLoggerNamePattern) || loggerName.startsWith(queryLoggerNamePattern)) {
                return true;
            }
        }

        return false;
    }

    static class BufferedMessage {

        private String level;

        private String msg;

        private Object[] arguments;

        private Throwable throwable;

        public BufferedMessage(String level, String msg) {
            this.level = level;
            this.msg = msg;
        }

        public BufferedMessage(String level, String msg, Object[] arguments) {
            this.level = level;
            this.msg = msg;
            this.arguments = arguments;
        }

        public BufferedMessage(String level, String msg, Throwable throwable) {
            this.level = level;
            this.msg = msg;
            this.throwable = throwable;
        }

        public void flush(Logger output) throws IOException {
            try {
                if (throwable != null) {
                    MethodUtils.invokeMethod(output, level.toLowerCase(), msg, throwable);
                } else if (arguments != null) {
                    MethodUtils.invokeMethod(output, level.toLowerCase(), msg, arguments);
                } else {
                    MethodUtils.invokeMethod(output, level.toLowerCase(), msg);
                }
            } catch (Exception ex) {
                throw new IOException(ex);
            }

        }
    }
}
