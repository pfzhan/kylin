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

package io.kyligence.kap.metadata.acl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class RowACLTest {
    @Test
    public void testBasic() {
        RowACL t1RowACL = new RowACL();
        t1RowACL.init("p", "user", "u1", "t1");
        t1RowACL.add(getColumnToConds());

        RowACL t2RowACL = new RowACL();
        t1RowACL.init("p", "user", "u1", "t2");
        t2RowACL.add(getColumnToConds());

        Assert.assertEquals(2, t1RowACL.size());
        Assert.assertEquals(2, t2RowACL.size());

        ColumnToConds columnToConds = t1RowACL.getColumnToConds();
        Assert.assertEquals(getColumnToConds(), columnToConds);
    }

    private static ColumnToConds getColumnToConds() {
        Map<String, List<ColumnToConds.Cond>> colToConds = new HashMap<>();
        List<ColumnToConds.Cond> cond1 = Lists.newArrayList(new ColumnToConds.Cond("a"), new ColumnToConds.Cond("b"),
                new ColumnToConds.Cond("c"));
        List<ColumnToConds.Cond> cond2 = Lists.newArrayList(new ColumnToConds.Cond("d"), new ColumnToConds.Cond("e"));
        colToConds.put("COUNTRY", cond1);
        colToConds.put("NAME", cond2);
        return new ColumnToConds(colToConds);
    }

}