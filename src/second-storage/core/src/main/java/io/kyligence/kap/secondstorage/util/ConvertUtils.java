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
package io.kyligence.kap.secondstorage.util;

import io.kyligence.kap.secondstorage.config.SecondStorageProjectModelSegment;

import java.util.Locale;

public class ConvertUtils {
    public static <T> T convertValue(Object rawValue, Class<?> clazz) {
        if (Integer.class.equals(clazz)) {
            return (T) convertToInt(rawValue);
        } else if (Long.class.equals(clazz)) {
            return (T) convertToLong(rawValue);
        } else if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(rawValue);
        } else if (Float.class.equals(clazz)) {
            return (T) convertToFloat(rawValue);
        } else if (Double.class.equals(clazz)) {
            return (T) convertToDouble(rawValue);
        } else if (String.class.equals(clazz)) {
            return (T) convertToString(rawValue);
        } else if (SecondStorageProjectModelSegment.class.equals(clazz)) {
            return (T) rawValue;
        }
        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }

    static String convertToString(Object o) {
        if (o.getClass() == String.class) {
            return (String) o;
        }
        return o.toString();
    }

    static Integer convertToInt(Object o) {
        if (o.getClass() == Integer.class) {
            return (Integer) o;
        } else if (o.getClass() == Long.class) {
            long value = (Long) o;
            if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
                return (int) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT,
                                "Configuration value %s overflow/underflow the integer type.",
                                value));
            }
        }
        return Integer.parseInt(o.toString());
    }

    static Long convertToLong(Object o) {
        if (o.getClass() == Long.class) {
            return (Long) o;
        } else if (o.getClass() == Integer.class) {
            return ((Integer) o).longValue();
        }
        return Long.parseLong(o.toString());
    }

    static Boolean convertToBoolean(Object o) {
        if (o.getClass() == Boolean.class) {
            return (Boolean) o;
        }

        switch (o.toString().toUpperCase(Locale.ROOT)) {
            case "TRUE":
                return true;
            case "FALSE":
                return false;
            default:
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT,
                                "Unrecognized option for boolean: %s. Expected either true or false(case insensitive)",
                                o));
        }
    }

    static Float convertToFloat(Object o) {
        if (o.getClass() == Float.class) {
            return (Float) o;
        } else if (o.getClass() == Double.class) {
            double value = ((Double) o);
            if (value == 0.0
                    || (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE)
                    || (value >= -Float.MAX_VALUE && value <= -Float.MIN_VALUE)) {
                return (float) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT,
                                "Configuration value %s overflows/underflow the float type.",
                                value));
            }
        }

        return Float.parseFloat(o.toString());
    }

    static Double convertToDouble(Object o) {
        if (o.getClass() == Double.class) {
            return (Double) o;
        } else if (o.getClass() == Float.class) {
            return ((Float) o).doubleValue();
        }

        return Double.parseDouble(o.toString());
    }
}
