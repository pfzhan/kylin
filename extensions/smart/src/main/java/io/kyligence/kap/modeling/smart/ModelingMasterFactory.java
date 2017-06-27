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

package io.kyligence.kap.modeling.smart;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;

import io.kyligence.kap.modeling.smart.query.QueryStats;
import io.kyligence.kap.modeling.smart.stats.ICubeStats;

public class ModelingMasterFactory {
    public static ModelingMaster create(KylinConfig kylinConfig, DataModelDesc modelDesc) {
        return create(kylinConfig, modelDesc, new String[0]);
    }

    public static ModelingMaster create(KylinConfig kylinConfig, CubeDesc cubeDesc) {
        return create(kylinConfig, cubeDesc, null);
    }

    public static ModelingMaster create(KylinConfig kylinConfig, DataModelDesc modelDesc, String[] sqls) {
        ModelingContextBuilder contextBuilder = new ModelingContextBuilder(kylinConfig);
        ModelingContext context = contextBuilder.buildFromModelDesc(modelDesc, sqls);
        return new ModelingMaster(context);
    }

    public static ModelingMaster create(KylinConfig kylinConfig, DataModelDesc modelDesc, QueryStats queryStats) {
        ModelingContextBuilder contextBuilder = new ModelingContextBuilder(kylinConfig);
        ModelingContext context = contextBuilder.buildFromModelDesc(modelDesc, queryStats);
        return new ModelingMaster(context);
    }

    public static ModelingMaster create(KylinConfig kylinConfig, CubeDesc cubeDesc, String[] sqls) {
        ModelingContextBuilder contextBuilder = new ModelingContextBuilder(kylinConfig);
        ModelingContext context = contextBuilder.buildFromCubeDesc(cubeDesc, sqls);
        return new ModelingMaster(context);
    }

    public static ModelingMaster create(KylinConfig kylinConfig, CubeDesc cubeDesc, ICubeStats cubeStats,
            QueryStats queryStats) {
        ModelingContextBuilder contextBuilder = new ModelingContextBuilder(kylinConfig);
        ModelingContext context = contextBuilder.buildFromCubeDesc(cubeDesc, cubeStats, queryStats);
        return new ModelingMaster(context);
    }
}
