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

package io.kyligence.kap.storage.parquet;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.storage.parquet.cube.raw.RawTableStorageQuery;
import io.kyligence.kap.storage.parquet.steps.ParquetMROutput2;

public class ParquetStorage implements IStorage, IKeep {
    @Override
    public IStorageQuery createQuery(IRealization realization) {
        switch (realization.getType()) {
        case CubeInstance.REALIZATION_TYPE:
            return new io.kyligence.kap.storage.parquet.cube.CubeStorageQuery((CubeInstance) realization);
        case RawTableInstance.REALIZATION_TYPE:
            return new RawTableStorageQuery((RawTableInstance) realization);
        default:
            throw new IllegalStateException(
                    "Unsupported realization type for ParquetStorage: " + realization.getType());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMROutput2.class) {
            return (I) new ParquetMROutput2();
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }
}
