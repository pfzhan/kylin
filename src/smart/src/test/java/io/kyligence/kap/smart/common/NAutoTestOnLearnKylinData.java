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
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.query.relnode.OLAPContext;
import org.junit.After;
import org.junit.Before;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.smart.NSmartContext;
import lombok.val;

public abstract class NAutoTestOnLearnKylinData {


    protected String proj = "learn_kylin";
    private File tmpMeta;
    private SetAndUnsetThreadLocalConfig localConfig;
    private FavoriteQueryManager favoriteQueryManager;

    @Before
    public void setUp() throws Exception {
        String metaDir = "src/test/resources/nsmart/learn_kylin/meta";
        tmpMeta = Files.createTempDir();
        FileUtils.copyDirectory(new File(metaDir), tmpMeta);

        Properties props = new Properties();
        props.setProperty("kylin.metadata.url", tmpMeta.getCanonicalPath());

        KylinConfig kylinConfig = KylinConfig.createKylinConfig(props);
        kylinConfig.setProperty("kylin.env", "UT");
        localConfig = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig);
        favoriteQueryManager = FavoriteQueryManager.getInstance(kylinConfig, proj);
    }

    public KylinConfig getTestConfig() {
        return localConfig.get();
    }

    @After
    public void tearDown() throws Exception {
        if (tmpMeta != null)
            FileUtils.forceDelete(tmpMeta);
        ResourceStore.clearCache(localConfig.get());
        localConfig.close();
    }

    protected List<LayoutEntity> collectAllLayouts(List<IndexEntity> indexEntities) {
        List<LayoutEntity> layouts = Lists.newArrayList();
        for (IndexEntity indexEntity : indexEntities) {
            layouts.addAll(indexEntity.getLayouts());
        }
        return layouts;
    }

    protected Set<FavoriteQueryRealization> collectFQR(List<LayoutEntity> layouts) {
        Set<FavoriteQueryRealization> realizations = Sets.newHashSet();
        layouts.forEach(layout -> {
            final long layoutId = layout.getId();
            final String modelId = layout.getModel().getId();

            val tmp = favoriteQueryManager.getFQRByConditions(modelId, layoutId);
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
        Set<FavoriteQuery> favoriteQueries = new HashSet<>();
        for (String sql : sqls) {
            FavoriteQuery fq = new FavoriteQuery(sql);
            favoriteQueries.add(fq);
        }

        favoriteQueryManager.create(favoriteQueries);
    }

}
