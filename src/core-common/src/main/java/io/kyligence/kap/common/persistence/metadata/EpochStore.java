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
package io.kyligence.kap.common.persistence.metadata;

import java.util.List;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

public abstract class EpochStore {
    public static final String EPOCH_SUFFIX = "_epoch";

    public abstract void update(Epoch epoch);

    public abstract void insert(Epoch epoch);

    public abstract void updateBatch(List<Epoch> epochs);

    public abstract void insertBatch(List<Epoch> epochs);

    public abstract Epoch getEpoch(String epochTarget);

    public abstract List<Epoch> list();

    public abstract void delete(String epochTarget);

    public abstract void createIfNotExist() throws Exception;

    public abstract <T> T executeWithTransaction(Callback<T> callback);

    public Epoch getGlobalEpoch() {
        return getEpoch("_global");
    }

    public static EpochStore getEpochStore(KylinConfig config) throws Exception {
        EpochStore epochStore = Singletons.getInstance(EpochStore.class, clz -> {
            if (Objects.equals(config.getMetadataUrl().getScheme(), "jdbc")) {
                return JdbcEpochStore.getEpochStore(config);
            } else {
                return FileEpochStore.getEpochStore(config);
            }
        });

        if (!config.isMetadataOnlyForRead()) {
            epochStore.createIfNotExist();
        }
        return epochStore;
    }

    public interface Callback<T> {
        T handle() throws Exception;

        default void onError() {
            // do nothing by default
        }
    }
}