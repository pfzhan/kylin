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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.util.PerformanceSensitive;

import io.kyligence.kap.common.obf.IKeep;

@Plugin(name = "SensitivePatternMasker", category = "Converter")
@ConverterKeys({ "mask" })
public class SensitivePatternMasker extends LogEventPatternConverter implements IKeep {
    private static final String PREFIX_GROUP_NAME = "prefix";
    private static final String SENSITIVE_GROUP_NAME = "sensitive";
    private static final String MASK = "******";
    private static final Pattern SENSITIVE_PATTERN = Pattern.compile(String.format(Locale.ROOT,
            "(?<%s>password\\s*[:=])(?<%s>[^,.!]*)", PREFIX_GROUP_NAME, SENSITIVE_GROUP_NAME),
            Pattern.CASE_INSENSITIVE);

    public static SensitivePatternMasker newInstance(final Configuration config, final String[] options) {
        return new SensitivePatternMasker();
    }

    protected SensitivePatternMasker() {
        super("mask", "mask");
    }

    private String mask(String message) {
        Matcher matcher = SENSITIVE_PATTERN.matcher(message);
        if (matcher.find()) {
            return matcher.replaceAll(String.format(Locale.ROOT, "${%s}%s", PREFIX_GROUP_NAME, MASK));
        }
        return message;
    }

    @PerformanceSensitive({ "allocation" })
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        toAppendTo.append(mask(event.getMessage().getFormattedMessage()));
    }
}
