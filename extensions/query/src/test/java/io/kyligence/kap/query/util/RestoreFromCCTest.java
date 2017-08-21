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

package io.kyligence.kap.query.util;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class RestoreFromCCTest {

    @Test
    public void replaceAliasInExpr() throws Exception {
        BiMap<String, String> mockMapping = HashBiMap.create();
        mockMapping.put("Q_X", "X");
        mockMapping.put("Q_Y", "Y");
        QueryAliasMatchInfo queryAliasMatchInfo = new QueryAliasMatchInfo(mockMapping, null);

        {
            String ret = RestoreFromComputedColumn.replaceAliasInExpr("X.m * Y.n", queryAliasMatchInfo);
            Assert.assertEquals("Q_X.M * Q_Y.N", ret);
        }
        {
            String ret = RestoreFromComputedColumn.replaceAliasInExpr("substr(X.m,1,3)>Y.n", queryAliasMatchInfo);
            Assert.assertEquals("substr(Q_X.M,1,3)>Q_Y.N", ret);
        }
        {
            String ret = RestoreFromComputedColumn.replaceAliasInExpr("strcmp(substr(Y.n,1,3),X.m)",
                    queryAliasMatchInfo);
            Assert.assertEquals("strcmp(substr(Q_Y.N,1,3),Q_X.M)", ret);
        }
    }

}
