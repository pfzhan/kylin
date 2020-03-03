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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.junit.Test;

public class SensitivePatternLayoutTest {

    @Test
    public void testMaskSensitiveData() {
        SensitivePatternLayout sensitivePatternLayout = new SensitivePatternLayout();

        Logger logger = Logger.getLogger("io.kyligence.kap.common.logging.SensitivePatternLayoutTest");

        LoggingEvent event = new LoggingEvent("org.apache.log4j.Logger", logger, Level.INFO,
                "kylin.database.password=kylin", null);
        String maskMessage = sensitivePatternLayout.format(event);
        Assert.assertEquals("kylin.database.password=******", maskMessage.trim());

        event = new LoggingEvent("org.apache.log4j.Logger", logger, Level.INFO, "password :  kylin", null);
        maskMessage = sensitivePatternLayout.format(event);
        Assert.assertEquals("password :******", maskMessage.trim());

        event = new LoggingEvent("org.apache.log4j.Logger", logger, Level.INFO, "PASSWORD =  kylin, password=kylin",
                null);
        maskMessage = sensitivePatternLayout.format(event);
        Assert.assertEquals("PASSWORD =******, password=******", maskMessage.trim());

        event = new LoggingEvent("org.apache.log4j.Logger", logger, Level.INFO, "PASSWORD=", null);
        maskMessage = sensitivePatternLayout.format(event);
        Assert.assertEquals("PASSWORD=******", maskMessage.trim());
    }

}