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
package io.kyligence.kap.newten.auto;

import com.google.common.collect.Maps;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.newten.auto.NAutoPerformanceTestBase.ProposeStats;

import java.util.Map;

public class NAutoSoakTestForJenkins {

    /**
     * inside kap-it/ dir, run: mvn exec:java -Dexec.mainClass="io.kyligence.kap.newten.auto.NAutoSoakTestForJenkins”
     * -Dexec.args="10 5" -Dexec.classpathScope=test
     *
     * @param args two args, first one is the round of soak test, second one is the factor for query generation
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        int round = Integer.valueOf(args[0]);
        int factor = Integer.valueOf(args[1]);

        NAutoPerformanceTestBase performanceTestBase = new NAutoPerformanceTestBase();
        // Prepare spark for CC
        NLocalWithSparkSessionTest.beforeClass();
        performanceTestBase.setup();

        Map<Integer, ProposeStats> proposeSummary = Maps.newHashMap();
        for (int i = 0; i < round; i++) {
            ProposeStats proposeStats = performanceTestBase.testWithBadQueries(i, factor);
            proposeSummary.put(i + 1, proposeStats);
        }
        performanceTestBase.printProposeSummary(proposeSummary, round);

        // clean up
        performanceTestBase.tearDown();
        NLocalWithSparkSessionTest.afterClass();
    }

}
