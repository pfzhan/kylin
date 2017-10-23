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

package io.kyligence.kap.smart.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerFactory;

public class ModelContextBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ModelContextBuilder.class);

    private KylinConfig kylinConfig;
    private String projectName;

    public ModelContextBuilder(KylinConfig kylinConfig, String project) {
        this.kylinConfig = kylinConfig;
        this.projectName = project;
    }

    public List<ModelContext> buildFromSQLs(String[] sqls) {
        return buildFromSQLs(sqls, null);
    }

    public List<ModelContext> buildFromSQLs(String[] sqls, TableDesc rootFactTbl) {
        if (ArrayUtils.isEmpty(sqls)) {
            return ListUtils.EMPTY_LIST;
        }

        try (AbstractQueryRunner extractor = QueryRunnerFactory.createForModelSuggestion(kylinConfig, sqls, 1,
                projectName)) {
            extractor.execute();

            return buildFromOLAPContexts(Arrays.asList(sqls), extractor.getAllOLAPContexts(), rootFactTbl);
        } catch (Exception e) {
            logger.error("Failed to get query stats. ", e);
            return null;
        }
    }

    public List<ModelContext> buildFromOLAPContexts(List<String> sqls, List<Collection<OLAPContext>> olapContexts,
            TableDesc rootFactTbl) {
        List<ModelTree> modelTrees = buildModelTrees(sqls, olapContexts, rootFactTbl);
        return buildFromModelTrees(modelTrees);
    }

    private List<ModelTree> buildModelTrees(List<String> sqls, List<Collection<OLAPContext>> olapContexts,
            TableDesc rootFactTbl) {
        return new GreedyModelTreesBuilder(kylinConfig, projectName).build(sqls, olapContexts, rootFactTbl);
    }

    public List<ModelContext> buildFromModelTrees(List<ModelTree> modelTrees) {
        List<ModelContext> result = Lists.newLinkedList();
        for (ModelTree tree : modelTrees) {
            ModelContext ctx = new ModelContext(kylinConfig, projectName, tree);
            result.add(ctx);
        }
        return result;
    }
}
