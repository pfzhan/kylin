/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  * 
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  * 
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 * /
 */

package io.kyligence.kap;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.routing.Candidate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Maps;

/**
 */
@RunWith(Parameterized.class)
public class ITKapCombinationTest extends ITKapKylinQueryTest {

    @BeforeClass
    public static void setUp() throws SQLException {

        printInfo("setUp in ITCombinationTest");
    }

    @AfterClass
    public static void tearDown() {
        printInfo("tearDown in ITCombinationTest");
        clean();
        Candidate.restorePriorities();
    }

    /**
     * return all config combinations, where first setting specifies join type
     * (inner or left), and the second setting specifies whether to force using
     * coprocessors(on, off or unset).
     */
    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { "inner", false }, { "left", false }, { "left", true } });
    }

    public ITKapCombinationTest(String joinType, Boolean rawTableFirst) throws Exception {

        if (rawTableFirst) {
            Map<RealizationType, Integer> priorities = Maps.newHashMap();
            priorities.put(RealizationType.HYBRID, 1);
            priorities.put(RealizationType.CUBE, 1);
            priorities.put(RealizationType.INVERTED_INDEX, 0);
            Candidate.setPriorities(priorities);
            ITKapKylinQueryTest.rawTableFirst = true;
        } else {
            Map<RealizationType, Integer> priorities = Maps.newHashMap();
            priorities.put(RealizationType.HYBRID, 0);
            priorities.put(RealizationType.CUBE, 0);
            priorities.put(RealizationType.INVERTED_INDEX, 0);
            Candidate.setPriorities(priorities);
            ITKapKylinQueryTest.rawTableFirst = false;
        }

        printInfo("Into combination join type: " + joinType);

        ITKapKylinQueryTest.clean();

        ITKapKylinQueryTest.joinType = joinType;
        ITKapKylinQueryTest.setupAll();
    }
}
