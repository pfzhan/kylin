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
package io.kyligence.kap.smart.query.validator;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;

import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerFactory;
import io.kyligence.kap.smart.query.advisor.CubeBasedSQLAdvisor;
import io.kyligence.kap.smart.query.advisor.ISQLAdvisor;

public class CubeSQLValidator extends AbstractSQLValidator {
    private CubeDesc cubeDesc;
    private ISQLAdvisor sqlAdvisor;

    public CubeSQLValidator(KylinConfig kylinConfig, CubeDesc cubeDesc) {
        super(kylinConfig);
        this.cubeDesc = cubeDesc;
        this.sqlAdvisor = new CubeBasedSQLAdvisor(cubeDesc);
    }

    @Override
    protected AbstractQueryRunner createQueryRunner(String[] sqls) {
        return QueryRunnerFactory.createForCubeSQLValid(kylinConfig, sqls, threadCount, cubeDesc);
    }

    @Override
    protected ISQLAdvisor getSQLAdvisor() {
        return sqlAdvisor;
    }
}
