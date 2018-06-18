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
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.filter.function;

import static org.apache.kylin.metadata.filter.function.LikeMatchers.LikeMatcher;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.reflect.MethodUtils;

import com.google.common.collect.ImmutableMap;

public enum BuiltInMethod {
    UPPER(BuiltInMethod.class, "upper", String.class), LOWER(BuiltInMethod.class, "lower", String.class), SUBSTRING(BuiltInMethod.class, "substring", String.class, int.class, int.class), CHAR_LENGTH(BuiltInMethod.class, "charLength", String.class), LIKE(BuiltInMethod.class, "like", String.class, String.class), INITCAP(BuiltInMethod.class, "initcap", String.class), CONCAT(BuiltInMethod.class, "concat", String.class, String.class);
    public final Method method;
    public static final ImmutableMap<String, BuiltInMethod> MAP;

    private static ThreadLocal<Map<String, LikeMatcher>> likePatterns = new ThreadLocal<Map<String, LikeMatcher>>() {
        @Override
        public Map<String, LikeMatcher> initialValue() {
            return new HashMap<>();
        }
    };

    static {
        final ImmutableMap.Builder<String, BuiltInMethod> builder = ImmutableMap.builder();
        for (BuiltInMethod value : BuiltInMethod.values()) {
            if (value.method != null) {
                builder.put(value.name(), value);
            }
        }
        MAP = builder.build();
    }

    BuiltInMethod(Class<?> clazz, String methodName, Class<?>... argumentTypes) {
        this.method = MethodUtils.getMatchingAccessibleMethod(clazz, methodName, argumentTypes);
    }

    /** SQL {@code LIKE} function. */
    public static boolean like(String s, String patternStr) {
        //TODO: escape in like is unsupported
        //TODO: like is case sensitive now

        if (s == null || patternStr == null)
            return false;

        Map<String, LikeMatcher> patterns = likePatterns.get();
        LikeMatcher p = patterns.get(patternStr);
        if (p == null) {

            p = LikeMatchers.createMatcher(patternStr);

            if (patterns.size() > 100) {
                patterns.clear();//brutal but good enough
            }
            patterns.put(patternStr, p);
        }

        return p.matches(s);
    }

    /** SQL INITCAP(string) function. */
    public static String initcap(String s) {
        // Assumes Alpha as [A-Za-z0-9]
        // white space is treated as everything else.
        final int len = s.length();
        boolean start = true;
        final StringBuilder newS = new StringBuilder();

        for (int i = 0; i < len; i++) {
            char curCh = s.charAt(i);
            final int c = (int) curCh;
            if (start) { // curCh is whitespace or first character of word.
                if (c > 47 && c < 58) { // 0-9
                    start = false;
                } else if (c > 64 && c < 91) { // A-Z
                    start = false;
                } else if (c > 96 && c < 123) { // a-z
                    start = false;
                    curCh = (char) (c - 32); // Uppercase this character
                }
                // else {} whitespace
            } else { // Inside of a word or white space after end of word.
                if (c > 47 && c < 58) { // 0-9
                    // noop
                } else if (c > 64 && c < 91) { // A-Z
                    curCh = (char) (c + 32); // Lowercase this character
                } else if (c > 96 && c < 123) { // a-z
                    // noop
                } else { // whitespace
                    start = true;
                }
            }
            newS.append(curCh);
        } // for each character in s
        return newS.toString();
    }

    /** SQL CHARACTER_LENGTH(string) function. */
    public static int charLength(String s) {
        return s.length();
    }

    /** SQL SUBSTRING(string FROM ... FOR ...) function. */
    public static String substring(String s, int from, int for_) {
        if (s == null)
            return null;
        return s.substring(from - 1, Math.min(from - 1 + for_, s.length()));
    }

    /** SQL UPPER(string) function. */
    public static String upper(String s) {
        if (s == null)
            return null;
        return s.toUpperCase();
    }

    /** SQL LOWER(string) function. */
    public static String lower(String s) {
        if (s == null)
            return null;
        return s.toLowerCase();
    }

    /** SQL left || right */
    public static String concat(String left, String right) {
        if (left == null)
            return right;
        if (right == null)
            return left;
        return left.concat(right);
    }

}
