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

package io.kyligence.kap.metadata.model.schema;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.schema.strategy.ComputedColumnStrategy;
import io.kyligence.kap.metadata.model.schema.strategy.MultiplePartitionStrategy;
import io.kyligence.kap.metadata.model.schema.strategy.OverWritableStrategy;
import io.kyligence.kap.metadata.model.schema.strategy.SchemaChangeStrategy;
import io.kyligence.kap.metadata.model.schema.strategy.TableColumnStrategy;
import io.kyligence.kap.metadata.model.schema.strategy.TableStrategy;
import io.kyligence.kap.metadata.model.schema.strategy.UnOverWritableStrategy;
import lombok.val;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ModelImportChecker {

    private static final List<SchemaChangeStrategy> strategies = Arrays.asList(new ComputedColumnStrategy(),
            new UnOverWritableStrategy(), new TableColumnStrategy(), new TableStrategy(), new OverWritableStrategy(),
            new MultiplePartitionStrategy());

    public static SchemaChangeCheckResult check(SchemaUtil.SchemaDifference difference,
            ImportModelContext importModelContext) {
        Set<String> importModels = NDataModelManager
                .getInstance(importModelContext.getImportKylinConfig(), importModelContext.getTargetProject())
                .listAllModelAlias().stream().map(model -> importModelContext.getNewModels().getOrDefault(model, model))
                .collect(Collectors.toSet());

        Set<String> originalModels = NDataModelManager
                .getInstance(KylinConfig.getInstanceFromEnv(), importModelContext.getTargetProject())
                .listAllModelAlias();

        val result = new SchemaChangeCheckResult();
        for (SchemaChangeStrategy strategy : strategies) {
            result.addMissingItems(strategy.missingItems(difference, importModels, originalModels));
            result.addNewItems(strategy.newItems(difference, importModels, originalModels));
            result.addReduceItems(strategy.reduceItems(difference, importModels, originalModels));
            result.addUpdateItems(strategy.updateItems(difference, importModels, originalModels));
            result.areEqual(strategy.areEqual(difference, importModels));
        }
        return result;
    }
}
