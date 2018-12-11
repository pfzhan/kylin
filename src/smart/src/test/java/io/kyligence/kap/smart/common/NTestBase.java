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

package io.kyligence.kap.smart.common;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.common.persistence.image.ImageStore;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.query.Utils;
import lombok.val;

public abstract class NTestBase {

    protected static final Logger logger = LoggerFactory.getLogger(NTestBase.class);

    protected String proj = "learn_kylin";
    protected File tmpMeta;
    protected KylinConfig kylinConfig;
    protected FavoriteQueryManager favoriteQueryManager;

    @Before
    public void setUp() throws Exception {
        String metaDir = "src/test/resources/nsmart/learn_kylin/meta";
        tmpMeta = Files.createTempDir();
        FileUtils.copyDirectory(new File(metaDir), new File(tmpMeta, ImageStore.METADATA_DIR));

        kylinConfig = Utils.smartKylinConfig(tmpMeta.getCanonicalPath() + ",mq=mock");
        kylinConfig.setProperty("kylin.env", "UT");
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
        favoriteQueryManager = FavoriteQueryManager.getInstance(kylinConfig, proj);
    }

    @After
    public void tearDown() throws Exception {
        KylinConfig.removeKylinConfigThreadLocal();
        if (tmpMeta != null)
            FileUtils.forceDelete(tmpMeta);
        ResourceStore.clearCache(kylinConfig);
    }

    protected <T> int countInnerObj(Collection<Collection<T>> collections) {
        if (collections == null)
            return 0;
        return collections.stream().flatMap(Collection::stream).collect(Collectors.toList()).size();
    }

    protected List<NCuboidLayout> collectAllLayouts(List<NCuboidDesc> cuboidDescs) {
        List<NCuboidLayout> layouts = Lists.newArrayList();
        for (NCuboidDesc cuboidDesc : cuboidDescs) {
            layouts.addAll(cuboidDesc.getLayouts());
        }
        return layouts;
    }

    protected Set<FavoriteQueryRealization> collectFavoriteQueryRealizations(List<NCuboidLayout> layouts) {
        Set<FavoriteQueryRealization> realizations = Sets.newHashSet();
        layouts.forEach(layout -> {
            final long layoutId = layout.getId();
            final String cubePlanId = layout.getCuboidDesc().getCubePlan().getId();
            final String modelId = layout.getModel().getId();

            val tmp = favoriteQueryManager.getRealizationsByConditions(modelId, cubePlanId, layoutId);
            realizations.addAll(tmp);
        });
        return realizations;
    }

    protected Set<OLAPContext> collectAllOlapContexts(NSmartContext smartContext) {
        Preconditions.checkArgument(smartContext != null);
        Set<OLAPContext> olapContexts = Sets.newHashSet();
        final List<NSmartContext.NModelContext> modelContexts = smartContext.getModelContexts();
        modelContexts.forEach(modelCtx -> olapContexts.addAll(modelCtx.getModelTree().getOlapContexts()));

        return olapContexts;
    }
    protected void initFQData(String[] sqls) {
        List<FavoriteQuery> favoriteQueries = Lists.newArrayList();
        for (String sql : sqls) {
            FavoriteQuery fq = new FavoriteQuery(sql);
            favoriteQueries.add(fq);
        }

        favoriteQueryManager.create(favoriteQueries);
    }
}
