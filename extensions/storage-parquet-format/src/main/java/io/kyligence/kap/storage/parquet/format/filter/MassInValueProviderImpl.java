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

package io.kyligence.kap.storage.parquet.format.filter;

import java.io.IOException;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.metadata.filter.UDF.MassInValueProvider;
import org.apache.kylin.metadata.filter.function.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.filter.MassinFilterManager;

public class MassInValueProviderImpl implements MassInValueProvider {
    public static final Logger logger = LoggerFactory.getLogger(MassInValueProviderImpl.class);

    private Set<ByteArray> ret = Sets.newHashSet();

    public MassInValueProviderImpl(Functions.FilterTableType filterTableType, String filterResourceIdentifier,
            DimensionEncoding encoding) throws IOException {
        MassinFilterManager manager = MassinFilterManager.getInstance(KylinConfig.getInstanceFromEnv());
        manager.setEncoding(filterResourceIdentifier, encoding);

        if (filterTableType == Functions.FilterTableType.HDFS) {
            ret = manager.load(filterTableType, filterResourceIdentifier);
        } else {
            throw new RuntimeException("HBASE_TABLE FilterTableType Not supported yet");
        }
    }

    @Override
    public Set<?> getMassInValues() {
        return ret;
    }
}
