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

package org.apache.kylin.metrics.lib.impl;

import java.util.Locale;

import org.apache.kylin.shaded.com.google.common.base.Strings;

public enum TimePropertyEnum {
    YEAR("KYEAR_BEGIN_DATE"),
    MONTH("KMONTH_BEGIN_DATE"),
    WEEK_BEGIN_DATE("KWEEK_BEGIN_DATE"),
    DAY_DATE("KDAY_DATE"),
    DAY_TIME("KDAY_TIME"),
    TIME_HOUR("KTIME_HOUR"),
    TIME_MINUTE("KTIME_MINUTE"),
    TIME_SECOND("KTIME_SECOND");

    private final String propertyName;

    TimePropertyEnum(String propertyName) {
        this.propertyName = propertyName;
    }

    public static TimePropertyEnum getByPropertyName(String propertyName) {
        if (Strings.isNullOrEmpty(propertyName)) {
            return null;
        }
        for (TimePropertyEnum property : TimePropertyEnum.values()) {
            if (property.propertyName.equals(propertyName.toUpperCase(Locale.ROOT))) {
                return property;
            }
        }
        return null;
    }

    public String toString() {
        return propertyName;
    }
}