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

package io.kyligence.kap.query;

import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.metadata.realization.RoutingIndicatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KapQueryAdvisor implements OLAPContext.IAccessController {
    private final static Logger logger = LoggerFactory.getLogger(KapQueryAdvisor.class);

    @Override
    public void check(List<OLAPContext> contexts, KylinConfig config) throws IllegalStateException {

        checkCalciteCost(contexts, config);
    }

    // Check cost within calcite context, route to pushdown if cost is too high
    private void checkCalciteCost(List<OLAPContext> kylinContexts, KylinConfig config) {
        int calciteJoinThreshold = getCalciteJoinThreshold(config);
        if (calciteJoinThreshold < 0)
            return;

        int nJoins = 0;
        
        for (int i = 0; i < OLAPContext.getThreadLocalContexts().size(); i++) {
            OLAPContext ctx = OLAPContext.getThreadLocalContextById(i);
            
            if (kylinContexts.contains(ctx))
                continue;
            
            nJoins += ctx.allOlapJoins.size();
        }
        
        if (nJoins > calciteJoinThreshold) {
            throw new RoutingIndicatorException("Detect high calcite cost, " + nJoins + " joins exceeding threshold "
                    + calciteJoinThreshold + ", route to pushdown");
        }
    }

    private int getCalciteJoinThreshold(KylinConfig config) {
        KapConfig cfg = KapConfig.wrap(config);
        
        int calciteJoinThreshold = cfg.getCalciteJoinThreshold();
        
        if (BackdoorToggles.getToggle("markCastProjectRemoved") != null) {
            calciteJoinThreshold = 0;
            logger.debug("Backdoor toggle 'markCastProjectRemoved' detected, set calciteJoinThreshold=0");
        }
        
        return calciteJoinThreshold;
    }
}
