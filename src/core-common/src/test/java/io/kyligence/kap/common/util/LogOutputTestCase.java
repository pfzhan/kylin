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
package io.kyligence.kap.common.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.After;
import org.junit.Before;

import lombok.val;

public class LogOutputTestCase extends NLocalFileMetadataTestCase {

    public static class MockAppender extends AbstractAppender {
        public List<String> events = new ArrayList<>();

        protected MockAppender() {
            super("mock", null, PatternLayout.createDefaultLayout(), true, new Property[0]);
        }

        public void close() {
        }

        public boolean requiresLayout() {
            return false;
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.getMessage().getFormattedMessage());
        }
    }

    @Before
    public void createLoggerAppender() {
        LoggerContext context = (LoggerContext) LogManager.getContext();
        Configuration configuration = context.getConfiguration();
        val loggerAppender = new MockAppender();
        configuration.addAppender(loggerAppender);
        Logger logger = (Logger) LogManager.getLogger("");
        logger.addAppender(configuration.getAppender("mock"));
    }

    @After
    public void removeLoggerAppender() {
        LoggerContext context = (LoggerContext) LogManager.getContext();
        Configuration configuration = context.getConfiguration();
        Logger logger = (Logger) LogManager.getLogger("");
        logger.removeAppender(configuration.getAppender("mock"));
        configuration.removeLogger("mock");
    }

    protected boolean containsLog(String log) {
        return getMockAppender().events.contains(log);
    }

    protected void clearLogs() {
        val appender = getMockAppender();
        if (appender != null) {
            appender.events.clear();
        }
    }

    MockAppender getMockAppender() {
        Logger logger = (Logger) LogManager.getLogger("");
        return (MockAppender) logger.getAppenders().get("mock");
    }
}