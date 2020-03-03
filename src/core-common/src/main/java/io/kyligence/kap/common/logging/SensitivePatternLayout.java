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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

public class SensitivePatternLayout extends PatternLayout {
    private static final String PREFIX_GROUP_NAME = "prefix";
    private static final String SENSITIVE_GROUP_NAME = "sensitive";
    private static final String MASK = "******";
    private static final Pattern SENSITIVE_PATTERN = Pattern.compile(
            String.format("(?<%s>password\\s*[:=])(?<%s>[^,.!]*)", PREFIX_GROUP_NAME, SENSITIVE_GROUP_NAME),
            Pattern.CASE_INSENSITIVE);

    @Override
    public String format(LoggingEvent event) {
        if (event.getMessage() instanceof String) {
            String maskedMessage = mask(event.getRenderedMessage());

            Throwable throwable = event.getThrowableInformation() != null
                    ? event.getThrowableInformation().getThrowable()
                    : null;
            LoggingEvent maskedEvent = new LoggingEvent(event.fqnOfCategoryClass,
                    Logger.getLogger(event.getLoggerName()), event.timeStamp, event.getLevel(), maskedMessage,
                    throwable);

            return super.format(maskedEvent);
        }
        return super.format(event);
    }

    private String mask(String message) {
        Matcher matcher = SENSITIVE_PATTERN.matcher(message);
        if (matcher.find()) {
            return matcher.replaceAll(String.format("${%s}%s", PREFIX_GROUP_NAME, MASK));
        }
        return message;
    }
}
