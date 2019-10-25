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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlOperator;

public class RexUtils {

    public static int countOperatorCall(RexNode condition, final Class<? extends SqlOperator> sqlOperator) {
        final AtomicInteger likeCount = new AtomicInteger(0);
        RexVisitor<Void> likeVisitor = new RexVisitorImpl<Void>(true) {
            public Void visitCall(RexCall call) {

                if (call.getOperator().getClass().equals(sqlOperator)) {
                    likeCount.incrementAndGet();
                }
                return (Void) super.visitCall(call);
            }
        };
        condition.accept(likeVisitor);
        return likeCount.get();
    }

    public static Set<RexInputRef> getAllInputRefs(RexNode rexNode) {
        if (rexNode instanceof RexInputRef) {
            return Collections.singleton((RexInputRef) rexNode);
        } else if (rexNode instanceof RexCall) {
            return getAllInputRefsCall((RexCall) rexNode);
        } else {
            return Collections.emptySet();
        }
    }

    private static Set<RexInputRef> getAllInputRefsCall(RexCall rexCall) {
        return rexCall.getOperands().stream()
                .flatMap(rexNode -> getAllInputRefs(rexNode).stream())
                .collect(Collectors.toSet());
    }
}
