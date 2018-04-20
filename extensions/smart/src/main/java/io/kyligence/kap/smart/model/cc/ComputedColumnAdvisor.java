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

package io.kyligence.kap.smart.model.cc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Litmus;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.model.ComputedColumnDesc;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ComputedColumnAdvisor extends SqlBasicVisitor {

    private static Logger logger = LoggerFactory.getLogger(ComputedColumnAdvisor.class);

    private final static IAdviceRule[] registeredRules = new IAdviceRule[] { 
            new SumAvgRule(), 
            new ArrayItemRule() 
            };

    Set<String> ccSuggestions = new HashSet<>();

    public List<String> suggestCandidate(String sql, List<ComputedColumnDesc> existedCCs) {
        ccSuggestions.clear();

        SqlNode sqlNode;
        try {
            sqlNode = CalciteParser.parse(sql);
            sqlNode.accept(this);
        } catch (SqlParseException e) {
            logger.error("Error in suggesting Computed Column", e);
        }

        if (existedCCs != null && !existedCCs.isEmpty()) {
            // remove duplicated CC expression
            for (ComputedColumnDesc cc : existedCCs) {
                String ccExpr = cc.getInnerExpression();
                SqlNode ccNode = CalciteParser.getExpNode(ccExpr);

                Iterator<String> iterator = ccSuggestions.iterator();
                while (iterator.hasNext()) {
                    String ccSuggestion = iterator.next();
                    SqlNode suggestedNode = CalciteParser.getExpNode(ccSuggestion);
                    if (ccNode.equalsDeep(suggestedNode, Litmus.IGNORE)) {
                        iterator.remove();
                    }
                }
            }
        }

        return new ArrayList(ccSuggestions);
    }

    @Override
    public Object visit(SqlIdentifier id) {
        return null;
    }

    @Override
    public Object visit(SqlCall call) {
        for (IAdviceRule rule : registeredRules) {
            String suggestedCC = rule.matches(call);
            if (StringUtils.isNotEmpty(suggestedCC)) {
                ccSuggestions.add(suggestedCC);
                return null;
            }
        }

        return call.getOperator().acceptCall(this, call);
    }
}
