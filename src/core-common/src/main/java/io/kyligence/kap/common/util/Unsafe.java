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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kyligence.kap.common.util;

import java.io.StringWriter;
import java.lang.reflect.AccessibleObject;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.directory.api.util.Strings;

import lombok.extern.slf4j.Slf4j;

/**
 * Contains methods that call JDK methods that the
 * <a href="https://github.com/policeman-tools/forbidden-apis">forbidden
 * APIs checker</a> does not approve of.
 *
 * <p>This class is excluded from the check, so methods called via this class
 * will not fail the build.
 */
@Slf4j
public class Unsafe {

    private Unsafe() {
    }

    /** Calls {@link System#exit}. */
    public static void systemExit(int status) {
        System.exit(status);
    }

    /** Calls {@link Object#notifyAll()}. */
    public static void notifyAll(Object o) {
        o.notifyAll();
    }

    /** Calls {@link Object#notify()}. */
    public static void notify(Object o) {
        o.notify();
    }

    /** Calls {@link Object#wait()}. */
    public static void wait(Object o) throws InterruptedException {
        o.wait();
    }

    public static void wait(Object o, long ms) throws InterruptedException {
        o.wait(ms);
    }

    /** Clears the contents of a {@link StringWriter}. */
    public static void clear(StringWriter sw) {
        // Included in this class because StringBuffer is banned.
        sw.getBuffer().setLength(0);
    }

    /** For {@link MessageFormat#format(String, Object...)} cannot set locale*/
    public static String format(Locale locale, String pattern, Object... arguments) {
        MessageFormat temp = new MessageFormat(pattern, locale);
        return temp.format(arguments);
    }

    public static String getUrlFromHttpServletRequest(HttpServletRequest request) {
        return request.getRequestURL().toString();
    }

    /** Reflection usage to work around access flags fails with SecurityManagers 
     * and likely will not work anymore on runtime classes in Java 9 */
    public static void changeAccessibleObject(AccessibleObject accessibleObject, boolean value) {
        accessibleObject.setAccessible(value);
    }

    /** Overwrite system property in test */
    public static void overwriteSystemProp(Map<String, String> systemProp, String key, String value) {
        if (systemProp != null) {
            systemProp.put(key, System.getProperty(key));
        }

        if (Strings.isEmpty(value)) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    /** Restore all system properties in test */
    public static void restoreAllSystemProp(Map<String, String> systemProp) {
        if (systemProp != null) {
            systemProp.forEach((prop, value) -> System.clearProperty(prop));
            systemProp.clear();
        }
    }

    /** Set system property */
    public static String setProperty(String property, String value) {
        return System.setProperty(property, value);
    }

    /** Clear system property */
    public static void clearProperty(String property) {
        System.clearProperty(property);
    }
}