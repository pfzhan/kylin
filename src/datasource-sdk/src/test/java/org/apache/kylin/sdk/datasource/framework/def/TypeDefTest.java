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
package org.apache.kylin.sdk.datasource.framework.def;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

public class TypeDefTest {
    @Test
    public void testInit() {
        {
            TypeDef t = defineType("DECIMAL(19,4)");
            Assert.assertEquals(19, t.getDefaultPrecision());
            Assert.assertEquals(4, t.getDefaultScale());
            Assert.assertEquals("DECIMAL(19,4)", t.buildString(30, 2));
            Assert.assertEquals("DECIMAL", t.getName());
            Assert.assertEquals(t.getId(), t.getId().toUpperCase());
        }
        {
            TypeDef t = defineType("DECIMAL($p,$s)");
            Assert.assertEquals(-1, t.getDefaultPrecision());
            Assert.assertEquals(-1, t.getDefaultScale());
            Assert.assertEquals("DECIMAL(19,4)", t.buildString(19, 4));
            Assert.assertEquals("DECIMAL", t.getName());
            Assert.assertEquals(t.getId(), t.getId().toUpperCase());
        }
    }

    private TypeDef defineType(String pattern) {
        TypeDef t = new TypeDef();
        t.setId(UUID.randomUUID().toString().toLowerCase());
        t.setExpression(pattern);
        t.init();
        return t;
    }
}
