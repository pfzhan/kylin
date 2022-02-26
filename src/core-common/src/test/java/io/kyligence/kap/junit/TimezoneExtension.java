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

import java.util.TimeZone;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

import lombok.AllArgsConstructor;
import lombok.val;

@AllArgsConstructor
class TimezoneExtension implements BeforeEachCallback, AfterEachCallback {

    private static final Namespace NAMESPACE = Namespace.create(TimezoneExtension.class);
    private static final String KEY = "DefaultTimeZone";
    private String timezone;

    @Override
    public void beforeEach(ExtensionContext context) {
        val store = context.getStore(NAMESPACE);
        setDefaultTimeZone(store, timezone);
    }

    private void setDefaultTimeZone(Store store, String zoneId) {
        TimeZone defaultTimeZone = createTimeZone(zoneId);
        // defer storing the current default time zone until the new time zone could be created from the configuration
        // (this prevents cases where misconfigured extensions store default time zone now and restore it later,
        // which leads to race conditions in our tests)
        storeDefaultTimeZone(store);
        TimeZone.setDefault(defaultTimeZone);
    }

    private static TimeZone createTimeZone(String timeZoneId) {
        TimeZone configuredTimeZone = TimeZone.getTimeZone(timeZoneId);
        // TimeZone::getTimeZone returns with GMT as fallback if the given ID cannot be understood
        if (configuredTimeZone.equals(TimeZone.getTimeZone("GMT")) && !timeZoneId.equals("GMT")) {
            throw new ExtensionConfigurationException(String.format(
                    "@DefaultTimeZone not configured correctly. " + "Could not find the specified time zone + '%s'. "
                            + "Please use correct identifiers, e.g. \"GMT\" for Greenwich Mean Time.",
                    timeZoneId));
        }
        return configuredTimeZone;
    }

    private void storeDefaultTimeZone(Store store) {
        store.put(KEY, TimeZone.getDefault());
    }

    @Override
    public void afterEach(ExtensionContext context) {
        resetDefaultTimeZone(context.getStore(NAMESPACE));
    }

    private void resetDefaultTimeZone(Store store) {
        TimeZone timeZone = store.get(KEY, TimeZone.class);
        // default time zone is null if the extension was misconfigured and execution failed in "before"
        if (timeZone != null)
            TimeZone.setDefault(timeZone);
    }

}
