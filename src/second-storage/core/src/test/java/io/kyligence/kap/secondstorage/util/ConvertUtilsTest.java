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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Locale;

public class ConvertUtilsTest {

    @Test
    public void testConvertValue() {
        Assertions.assertEquals(Float.valueOf("1.22"), ConvertUtils.convertValue(1.22, Float.class));
        Assertions.assertEquals(Float.valueOf("1.30"), ConvertUtils.convertValue(1.30, Float.class));

        Assertions.assertEquals(Double.valueOf("1.22"), ConvertUtils.convertValue(1.22, Double.class));
        Assertions.assertEquals(Double.valueOf("1.30"), ConvertUtils.convertValue(1.30, Double.class));

        SecondStorageProjectModelSegment segment = new SecondStorageProjectModelSegment();
        ConvertUtils.convertValue(segment, SecondStorageProjectModelSegment.class);

        Assertions.assertThrows(IllegalArgumentException.class, () -> ConvertUtils.convertValue(segment, UnsupportedClass.class));

        ConvertUtils.convertValue(segment, SecondStorageProjectModelSegment.class);
    }

    @Test
    public void testConvertValueBoolean() {
        Assertions.assertEquals(true, ConvertUtils.convertValue(true, Boolean.class));
        Assertions.assertEquals(false, ConvertUtils.convertValue(false, Boolean.class));

        Assertions.assertEquals(true, ConvertUtils.convertValue("true", Boolean.class));
        Assertions.assertEquals(false, ConvertUtils.convertValue("false", Boolean.class));

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> ConvertUtils.convertValue("10", Boolean.class));
        Assertions.assertEquals(exception.getMessage(), String.format(Locale.ROOT,
                "Unrecognized option for boolean: %s. Expected either true or false(case insensitive)",
                10));
    }

    @Test
    public void testConvertValueLong() {
        Assertions.assertEquals(Long.valueOf(10), ConvertUtils.convertValue(Long.valueOf(10), Long.class));
        Assertions.assertEquals(Long.valueOf(10), ConvertUtils.convertValue(Integer.valueOf(10), Long.class));
        Assertions.assertEquals(Long.valueOf(10), ConvertUtils.convertValue("10", Long.class));
    }

    @Test
    public void testConvertValueInteger() {
        Assertions.assertEquals(Integer.valueOf(10), ConvertUtils.convertValue(Integer.valueOf(10), Integer.class));
        Assertions.assertEquals(Integer.valueOf(10), ConvertUtils.convertValue(Long.valueOf(10), Integer.class));
        Assertions.assertEquals(Integer.valueOf(10), ConvertUtils.convertValue("10", Integer.class));
        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> ConvertUtils.convertValue(Long.MAX_VALUE, Integer.class));
        Assertions.assertEquals(exception.getMessage(), String.format(Locale.ROOT,
                "Configuration value %s overflow/underflow the integer type.",
                Long.MAX_VALUE));
    }

    public class UnsupportedClass {

    }
}
