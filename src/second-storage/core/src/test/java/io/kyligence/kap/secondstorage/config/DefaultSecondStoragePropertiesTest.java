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
package io.kyligence.kap.secondstorage.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class DefaultSecondStoragePropertiesTest {

    @Test
    public void testGetOptional() {
        Properties properties = new Properties();
        properties.put("k1", "v1");
        SecondStorageProperties secondStorageProperties = new DefaultSecondStorageProperties(properties);

        Assertions.assertEquals("v1", secondStorageProperties.get(new ConfigOption<>("k1", "", String.class)));

        Assertions.assertEquals("v2", secondStorageProperties.get(new ConfigOption<>("k2", "v2", String.class)));


        Exception exception = Assertions.assertThrows(NullPointerException.class, () -> secondStorageProperties.get(new ConfigOption<>(null, String.class)));
        Assertions.assertEquals(exception.getMessage(), "Key must not be null.");


        Exception exception2 = Assertions.assertThrows(IllegalArgumentException.class, () -> secondStorageProperties.get(new ConfigOption<>("k1", MyClass.class)));
        Assertions.assertEquals(exception2.getMessage(), "Could not parse value 'v1' for key 'k1'.");

    }

    // for ut
    public class MyClass {

    }
}
