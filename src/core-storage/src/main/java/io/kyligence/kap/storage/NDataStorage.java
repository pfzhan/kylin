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

package io.kyligence.kap.storage;

import io.kyligence.kap.cube.model.NDataflow;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;

public class NDataStorage implements IStorage {
    @Override
    public IStorageQuery createQuery(IRealization realization) {
        switch (realization.getType()) {
        case NDataflow.REALIZATION_TYPE:
            return new NDataStorageQuery((NDataflow) realization);
        default:
            throw new IllegalStateException("Unsupported realization type for NDataStorage: " + realization.getType());
        }
    }

    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        Class clz;
        try {
            clz = Class.forName("io.kyligence.kap.engine.spark.NSparkCubingEngine$NSparkCubingStorage");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        if (engineInterface == clz) {
            return (I) ClassUtil.newInstance("io.kyligence.kap.engine.spark.storage.NParquetStorage");
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }

}
