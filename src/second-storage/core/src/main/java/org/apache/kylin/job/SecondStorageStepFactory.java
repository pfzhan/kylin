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

package org.apache.kylin.job;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.kyligence.kap.job.execution.AbstractExecutable;

public class SecondStorageStepFactory {
    private static final Map<Class<? extends SecondStorageStep>, Supplier<AbstractExecutable>> SUPPLIER_MAP = new ConcurrentHashMap<>(5);

    public static void register(Class<? extends SecondStorageStep> stepType, Supplier<AbstractExecutable> stepSupplier) {
        SUPPLIER_MAP.put(stepType, stepSupplier);
    }
    public static AbstractExecutable create(Class<? extends SecondStorageStep> stepType, Consumer<AbstractExecutable> paramInjector) {
        AbstractExecutable step = SUPPLIER_MAP.get(stepType).get();
        paramInjector.accept(step);
        return step;
    }

    public interface SecondStorageStep {
    }

    public interface SecondStorageLoadStep extends SecondStorageStep {

    }

    public interface SecondStorageRefreshStep extends SecondStorageStep {}

    public interface SecondStorageMergeStep extends SecondStorageStep {}

}
