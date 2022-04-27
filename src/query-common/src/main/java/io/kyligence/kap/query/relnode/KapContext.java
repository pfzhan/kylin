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

import java.util.Collection;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kylin.query.relnode.OLAPContext;

public class KapContext {
    private KapContext() {
    }

    static final ThreadLocal<KapRel> _inputRel = new ThreadLocal<>();
    static final ThreadLocal<RelDataType> _resultType = new ThreadLocal<>();

    public static void setKapRel(KapRel kapRel) {
        _inputRel.set(kapRel);
    }

    public static KapRel getKapRel() {
        return _inputRel.get();
    }

    public static void setRowType(RelDataType relDataType) {
        _resultType.set(relDataType);
    }

    public static RelDataType getRowType() {
        return _resultType.get();
    }

    public static void clean() {
        _inputRel.set(null);
        _resultType.remove();
    }

    public static void amendAllColsIfNoAgg(RelNode kapRel) {
        if (kapRel == null || ((KapRel) kapRel).getContext() == null || kapRel instanceof KapTableScan)
            return;

        OLAPContext context = ((KapRel) kapRel).getContext();
        // add columns of context's TopNode to context when there are no agg rel
        if (kapRel instanceof KapProjectRel && !((KapProjectRel) kapRel).isMerelyPermutation()) {
            ((KapRel) kapRel).getColumnRowType().getSourceColumns().stream().flatMap(Collection::stream)
                    .filter(context::isOriginAndBelongToCtxTables).forEach(context.allColumns::add);
        } else if (kapRel instanceof KapValuesRel) {
            ((KapRel) kapRel).getColumnRowType().getAllColumns().stream().filter(context::isOriginAndBelongToCtxTables)
                    .forEach(context.allColumns::add);
        } else if (kapRel instanceof KapWindowRel) {
            ((KapWindowRel) kapRel).getGroupingColumns().stream().filter(context::isOriginAndBelongToCtxTables)
                    .forEach(context.allColumns::add);
        } else if (kapRel instanceof KapJoinRel) {
            amendAllColsIfNoAgg(kapRel.getInput(0));
            amendAllColsIfNoAgg(kapRel.getInput(1));
        } else {
            amendAllColsIfNoAgg(kapRel.getInput(0));
        }
    }
}
