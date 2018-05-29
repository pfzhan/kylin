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

import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.metadata.filter.UDF.MassInValueProvider;
import org.apache.kylin.metadata.filter.UDF.MassInValueProviderFactory;
import org.apache.kylin.metadata.filter.function.Functions;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MassInValueProviderFactoryImpl implements MassInValueProviderFactory {
    public static final Logger logger = LoggerFactory.getLogger(MassInValueProviderFactoryImpl.class);

    public interface DimEncAware {
        DimensionEncoding getDimEnc(TblColRef col);
    }

    private DimEncAware dimEncAware = null;

    public MassInValueProviderFactoryImpl(DimEncAware dimEncAware) {
        this.dimEncAware = dimEncAware;
    }

    @Override
    public MassInValueProvider getProvider(Functions.FilterTableType filterTableType, String filterResourceIdentifier, TblColRef col) {
        if (dimEncAware != null) {
            try {
                return new MassInValueProviderImpl(filterTableType, filterResourceIdentifier, dimEncAware.getDimEnc(col));
            } catch (IOException e) {
                logger.error("{}", e);
            }
        } else {
            try {
                return new MassInValueProviderImpl(filterTableType, filterResourceIdentifier, null);
            } catch (IOException e) {
                logger.error("{}", e);
            }
        }
        return null;
    }
}
