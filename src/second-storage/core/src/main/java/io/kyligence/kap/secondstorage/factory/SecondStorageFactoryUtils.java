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
package io.kyligence.kap.secondstorage.factory;

import com.google.common.base.Preconditions;
import io.kyligence.kap.secondstorage.config.SecondStorageProperties;
import io.kyligence.kap.secondstorage.database.DatabaseOperator;
import io.kyligence.kap.secondstorage.database.QueryOperator;
import io.kyligence.kap.secondstorage.metadata.MetadataOperator;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SecondStorageFactoryUtils {
    private static final Map<Class<? extends SecondStorageFactory>, SecondStorageFactory> FACTORY_MAP = new ConcurrentHashMap<>();

    public static void register(Class<? extends SecondStorageFactory> type, SecondStorageFactory factory) {
        Preconditions.checkArgument(type.isAssignableFrom(factory.getClass()), String.format(Locale.ROOT, "type %s is not assignable from %s",
                type.getName(), factory.getClass().getName()));
        FACTORY_MAP.put(type, factory);
    }


    public static MetadataOperator createMetadataOperator(SecondStorageProperties properties) {
        SecondStorageMetadataFactory factory = (SecondStorageMetadataFactory) FACTORY_MAP.get(SecondStorageMetadataFactory.class);
        return factory.createMetadataOperator(properties);
    }

    public static DatabaseOperator createDatabaseOperator(String jdbcUrl) {
        SecondStorageDatabaseOperatorFactory factory = (SecondStorageDatabaseOperatorFactory) FACTORY_MAP.get(SecondStorageDatabaseOperatorFactory.class);
        return factory.createDatabaseOperator(jdbcUrl);
    }

    public static QueryOperator createQueryMetricOperator(String project) {
        SecondStorageQueryOperatorFactory factory = (SecondStorageQueryOperatorFactory) FACTORY_MAP.get(SecondStorageQueryOperatorFactory.class);
        return factory.getQueryOperator(project);
    }
}
