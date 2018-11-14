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

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.After;
import org.junit.Before;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import io.kyligence.kap.common.util.KylinConfigUtils;
import io.kyligence.kap.cube.model.NCuboidDesc;
import io.kyligence.kap.cube.model.NCuboidLayout;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealization;
import io.kyligence.kap.metadata.favorite.FavoriteQueryRealizationJDBCDao;
import io.kyligence.kap.smart.query.Utils;
import lombok.val;

public abstract class NTestBase {

    protected String proj = "learn_kylin";
    protected File tmpMeta;
    protected KylinConfig kylinConfig;
    protected FavoriteQueryRealizationJDBCDao dao;

    @Before
    public void setUp() throws Exception {
        String metaDir = "src/test/resources/nsmart/learn_kylin/meta";
        tmpMeta = Files.createTempDir();
        FileUtils.copyDirectory(new File(metaDir), tmpMeta);

        kylinConfig = Utils.smartKylinConfig(tmpMeta.getCanonicalPath());
        KylinConfigUtils.setH2DriverAsFavoriteQueryStorageDB(kylinConfig);
        KylinConfig.setKylinConfigThreadLocal(kylinConfig);
        dao = FavoriteQueryRealizationJDBCDao.getInstance(kylinConfig, proj);
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

            val tmp = dao.getByConditions(modelId, cubePlanId, layoutId);
            realizations.addAll(tmp);
        });
        return realizations;
    }
}
