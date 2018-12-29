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
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.query.relnode.OLAPContext;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.smart.NSmartContext;
import io.kyligence.kap.smart.query.Utils;
import lombok.val;

public abstract class NTestBase {

    protected static final Logger logger = LoggerFactory.getLogger(NTestBase.class);

    protected String proj = "learn_kylin";
    private File tmpMeta;
    private SetAndUnsetThreadLocalConfig localConfig;
    private FavoriteQueryManager favoriteQueryManager;

    @Before
    public void setUp() throws Exception {
        String metaDir = "src/test/resources/nsmart/learn_kylin/meta";
        tmpMeta = Files.createTempDir();
        FileUtils.copyDirectory(new File(metaDir), new File(tmpMeta, MetadataStore.METADATA_NAMESPACE));

        KylinConfig kylinConfig = Utils.smartKylinConfig(tmpMeta.getCanonicalPath());
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

    protected void showTableExdInfo() throws IOException {
        KylinConfig oldKylinConfig = localConfig.get();
        val resourceStore = ResourceStore.getKylinMetaStore(oldKylinConfig);
        ResourceStore.dumpResources(oldKylinConfig, tmpMeta, resourceStore.listResourcesRecursively("/"));

        reAddMetadataTableExd();

        KylinConfig kylinConfig = Utils.smartKylinConfig(tmpMeta.getCanonicalPath());
        kylinConfig.setProperty("kylin.env", "UT");
        localConfig.close();
        localConfig = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig);
        favoriteQueryManager = FavoriteQueryManager.getInstance(kylinConfig, proj);
    }

    protected void hideTableExdInfo() throws IOException {
        deleteMetadataTableExd();

        KylinConfig kylinConfig = Utils.smartKylinConfig(tmpMeta.getCanonicalPath());
        kylinConfig.setProperty("kylin.env", "UT");
        localConfig.close();
        localConfig = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig);
        favoriteQueryManager = FavoriteQueryManager.getInstance(kylinConfig, proj);
    }

    // ================== handle table exd ==============
    private String tmpTableExdDir;

    private void deleteMetadataTableExd() throws IOException {
        Preconditions.checkNotNull(tmpMeta, "no valid metadata.");
        val metaDir = new File(tmpMeta, MetadataStore.METADATA_NAMESPACE);
        final File[] files = metaDir.listFiles();
        Preconditions.checkNotNull(files);
        for (File file : files) {
            if (!file.isDirectory() || !file.getName().equals(proj)) {
                continue;
            }

            final File[] directories = file.listFiles();
            Preconditions.checkNotNull(directories);
            for (File item : directories) {
                if (item.isDirectory() && item.getName().equals("table_exd")) {
                    final File destTableExd = new File(metaDir.getParent(), "table_exd");
                    tmpTableExdDir = destTableExd.getCanonicalPath();
                    if (destTableExd.exists()) {
                        FileUtils.forceDelete(destTableExd);
                    }
                    FileUtils.moveDirectory(new File(file.getCanonicalPath(), "table_exd"), destTableExd);
                    return;
                }
            }
        }
    }

    private void reAddMetadataTableExd() throws IOException {
        Preconditions.checkNotNull(tmpMeta, "no valid metadata.");
        val metaDir = new File(tmpMeta, MetadataStore.METADATA_NAMESPACE);
        final File[] files = metaDir.listFiles();
        Preconditions.checkNotNull(files);
        for (File file : files) {
            if (file.isDirectory() && file.getName().equals(proj)) {
                File srcTableExd = new File(tmpTableExdDir);
                if (srcTableExd.exists()) {
                    FileUtils.copyDirectory(srcTableExd, new File(file.getCanonicalPath(), "table_exd"));
                }
                break;
            }
        }
    }
}
