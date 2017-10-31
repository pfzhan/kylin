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

package io.kyligence.kap.engine.spark;

import java.util.Set;

import org.apache.kylin.common.util.ImplementationSwitch;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.cube.model.NDataCuboid;
import io.kyligence.kap.cube.model.NDataSegment;
import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.job.engine.NCubingEngine;

public class NSparkCubingEngine implements NCubingEngine, IKeep {

    private static ThreadLocal<ImplementationSwitch<NSparkCubingStorage>> storages = new ThreadLocal<>();

    @Override
    public DefaultChainedExecutable createCubingJob(Set<NDataSegment> segments, Set<NCuboidLayout> cuboids,
            String submitter) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<?> getSourceInterface() {
        return NSparkCubingSource.class;
    }

    @Override
    public Class<?> getStorageInterface() {
        return NSparkCubingStorage.class;
    }

    public interface NSparkCubingSource {
        /**
         * Get Dataset<Row>
         *
         * @param dataflow, used to retrieve the columns
         * @param range, define the data range
         * @param ss
         * @return the Dataset<Row>, its schema consists of column's index, for example, [0,1,2,4]
         */
        Dataset<Row> getSourceData(NDataflow dataflow, @SuppressWarnings("rawtypes") SegmentRange range,
                SparkSession ss);

        /**
         * Get Dataset<Row>
         *
         * @param table, source table
         * @param ss
         * @return the Dataset<Row>, its schema consists of table column's name, for example, [column1,column2,column3]
         */
        Dataset<Row> getSourceData(TableDesc table, SparkSession ss);
    }

    public interface NSparkCubingStorage {

        Dataset<Row> getCuboidData(NDataCuboid cuboid, SparkSession ss);

        void saveCuboidData(NDataCuboid cuboid, Dataset<Row> data, SparkSession ss);
    }
}
