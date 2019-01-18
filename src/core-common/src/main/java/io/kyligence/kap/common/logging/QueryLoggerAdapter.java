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

import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.spi.LocationAwareLogger;

import java.io.Serializable;

public class QueryLoggerAdapter extends MarkerIgnoringBase implements LocationAwareLogger, Serializable {

    private transient Logger originalLogger;

    public QueryLoggerAdapter(Logger originalLogger) {
        this.originalLogger = originalLogger;
        this.name = originalLogger.getName();

    }

    @Override
    public void log(Marker marker, String fqcn, int level, String message, Object[] argArray, Throwable t) {
        if (originalLogger instanceof LocationAwareLogger) {
            ((LocationAwareLogger) originalLogger).log(marker, fqcn, level, message, argArray, t);
        }
    }

    @Override
    public boolean isTraceEnabled() {
        return originalLogger.isTraceEnabled();
    }

    @Override
    public void trace(String msg) {
        originalLogger.trace(msg);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.TRACE.toString(), msg);
    }

    @Override
    public void trace(String format, Object arg) {
        originalLogger.trace(format, arg);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.TRACE.toString(), format, arg);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        originalLogger.trace(format, arg1, arg2);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.TRACE.toString(), format, arg1, arg2);
    }

    @Override
    public void trace(String format, Object... arguments) {
        originalLogger.trace(format, arguments);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.TRACE.toString(), format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        originalLogger.trace(msg, t);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.TRACE.toString(), msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return originalLogger.isDebugEnabled();
    }

    @Override
    public void debug(String msg) {
        if (!QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.DEBUG.toString(), msg)) {
            originalLogger.debug(msg);
        }
    }

    @Override
    public void debug(String format, Object arg) {
        if (!QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.DEBUG.toString(), format, arg)) {
            originalLogger.debug(format, arg);
        }
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        if (!QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.DEBUG.toString(), format, arg1, arg2)) {
            originalLogger.debug(format, arg1, arg2);
        }
    }

    @Override
    public void debug(String format, Object... arguments) {
        if (!QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.DEBUG.toString(), format, arguments)) {
            originalLogger.debug(format, arguments);
        }
    }

    @Override
    public void debug(String msg, Throwable t) {
        if (!QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.DEBUG.toString(), msg, t)) {
            originalLogger.debug(msg, t);
        }
    }

    @Override
    public boolean isInfoEnabled() {
        return originalLogger.isInfoEnabled();
    }

    @Override
    public void info(String msg) {
        originalLogger.info(msg);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.INFO.toString(), msg);
    }

    @Override
    public void info(String format, Object arg) {
        originalLogger.info(format, arg);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.INFO.toString(), format, arg);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        originalLogger.info(format, arg1, arg2);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.INFO.toString(), format, arg1, arg2);
    }

    @Override
    public void info(String format, Object... arguments) {
        originalLogger.info(format, arguments);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.INFO.toString(), format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
        originalLogger.info(msg, t);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.INFO.toString(), msg, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return originalLogger.isWarnEnabled();
    }

    @Override
    public void warn(String msg) {
        originalLogger.warn(msg);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.WARN.toString(), msg);
    }

    @Override
    public void warn(String format, Object arg) {
        originalLogger.warn(format, arg);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.WARN.toString(), format, arg);
    }

    @Override
    public void warn(String format, Object... arguments) {
        originalLogger.warn(format, arguments);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.WARN.toString(), format, arguments);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        originalLogger.warn(format, arg1, arg2);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.WARN.toString(), format, arg1, arg2);
    }

    @Override
    public void warn(String msg, Throwable t) {
        originalLogger.warn(msg, t);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.WARN.toString(), msg, t);
    }

    @Override
    public boolean isErrorEnabled() {
        return originalLogger.isErrorEnabled();
    }

    @Override
    public void error(String msg) {
        originalLogger.error(msg);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.ERROR.toString(), msg);
    }

    @Override
    public void error(String format, Object arg) {
        originalLogger.error(format, arg);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.ERROR.toString(), format, arg);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        originalLogger.error(format, arg1, arg2);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.ERROR.toString(), format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... arguments) {
        originalLogger.error(format, arguments);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.ERROR.toString(), format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
        originalLogger.error(msg, t);
        QueryLoggerBufferUtil.bufferIfNecessary(this.name, Level.ERROR.toString(), msg, t);
    }
}
