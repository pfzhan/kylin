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

package io.kyligence.kap.metadata.model;

import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

public class ComputedColumnDescTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void simpleParserCheckTestSuccess1() {
        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("a.x + b.y", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail1() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(
                "Unrecognized column: C.Y in expression 'a.x + c.y'. When referencing a column, expressions should use patterns like ALIAS.COLUMN, where ALIAS is the table alias defined in model.");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("a.x + c.y", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail2() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: SUM");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("sum(a.x) * 10", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail3() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: MIN");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("min(a.x) + 10", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail4() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: MAX");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("max(a.x + b.y)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail5() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: COUNT");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("count(*)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail6() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: COUNT");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("count(a.x)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail7() {
        //value window function
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: LEAD");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("lead(a.x,1) over (order by a.y)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail8() {
        //ranking window function
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: NTILE");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("ntile(5) over(order by a.x)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail9() {
        //aggregate function
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: AVG");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("avg(a.x) over (partition by a.y) ", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail10() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain keyword AS");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("avg(a.x) over (partition by a.y) as xxx", aliasSet);
    }


    @Test
    public void simpleParserCheckTestFail11() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: COUNT");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("count(distinct a.x)", aliasSet);
    }

}
