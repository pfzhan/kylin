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

package io.kyligence.kap.query.relnode;

import org.apache.calcite.rel.RelNode;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPLimitRel;
import org.apache.kylin.query.relnode.OLAPSortRel;
import org.apache.kylin.query.relnode.OLAPValuesRel;
import org.apache.kylin.query.relnode.OLAPWindowRel;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;

public class ContextUtilTest {

    //https://github.com/Kyligence/KAP/issues/9952
    //do not support agg pushdown if WindowRel, SortRel, LimitRel, ValueRel is met
    @Test
    public void testDerivedFromSameContextWhenMetWindowOrSort() throws Exception {
        Method derivedMethod = ContextUtil.class.getDeclaredMethod("derivedFromSameContext", Collection.class, RelNode.class,
                OLAPContext.class, boolean.class);
        derivedMethod.setAccessible(true);
        {
            RelNode rel = Mockito.mock(OLAPWindowRel.class);
            Object result = derivedMethod.invoke(null, Collections.EMPTY_LIST, rel, null, false);
            Assert.assertEquals(false, result);
        }
        {
            RelNode rel = Mockito.mock(OLAPSortRel.class);
            Object result = derivedMethod.invoke(null, Collections.EMPTY_LIST, rel, null, false);
            Assert.assertEquals(false, result);
        }
        {
            RelNode rel = Mockito.mock(OLAPLimitRel.class);
            Object result = derivedMethod.invoke(null, Collections.EMPTY_LIST, rel, null, false);
            Assert.assertEquals(false, result);
        }
        {
            RelNode rel = Mockito.mock(OLAPValuesRel.class);
            Object result = derivedMethod.invoke(null, Collections.EMPTY_LIST, rel, null, false);
            Assert.assertEquals(false, result);
        }
    }
}
