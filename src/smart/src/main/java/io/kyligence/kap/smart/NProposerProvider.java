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

package io.kyligence.kap.smart;

public class NProposerProvider {
    private NSmartContext context;

    private NProposerProvider(NSmartContext context) {
        this.context = context;
    }

    public static NProposerProvider create(NSmartContext context) {
        return new NProposerProvider(context);
    }

    public NAbstractProposer getSQLAnalysisProposer() {
        return new NSQLAnalysisProposer(context);
    }

    public NAbstractProposer getModelSelectProposer() {
        return new NModelSelectProposer(context);
    }

    public NAbstractProposer getModelOptProposer() {
        return new NModelOptProposer(context);
    }

    public NAbstractProposer getCubePlanSelectProposer() {
        return new NCubePlanSelectProposer(context);
    }

    public NAbstractProposer getCubePlanOptProposer() {
        return new NCubePlanOptProposer(context);
    }
    
    public NAbstractProposer getCubePlanShrinkProposer() {
        return new NCubePlanShrinkProposer(context);
    }
    
    public NAbstractProposer getModelShrinkProposer() {
        return new NModelShrinkProposer(context);
    }
}
    
