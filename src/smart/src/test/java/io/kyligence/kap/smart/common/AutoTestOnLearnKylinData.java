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
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AbstractTestCase;
import org.apache.kylin.query.relnode.OLAPContext;
import org.junit.After;
import org.junit.Before;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import io.kyligence.kap.metadata.cube.model.IndexEntity;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.smart.AbstractContext;

public abstract class AutoTestOnLearnKylinData extends AbstractTestCase {

    protected String proj = "learn_kylin";
    private File tmpMeta;
    private SetAndUnsetThreadLocalConfig localConfig;

    @Before
    public void setUp() throws Exception {
        overwriteSystemProp("spark.local", "true");
        String metaDir = "src/test/resources/nsmart/learn_kylin/meta";
        File tmpHome = Files.createTempDir();
        tmpMeta = new File(tmpHome, "metadata");
        overwriteSystemProp("KYLIN_HOME", tmpHome.getAbsolutePath());
        FileUtils.copyDirectory(new File(metaDir), tmpMeta);
        FileUtils.touch(new File(tmpHome.getAbsolutePath() + "/kylin.properties"));
        KylinConfig.setKylinConfigForLocalTest(tmpHome.getCanonicalPath());
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.query.security.acl-tcr-enabled", "false");
        kylinConfig.setProperty("kylin.smart.conf.propose-runner-type", "in-memory");
        kylinConfig.setProperty("kylin.env", "UT");
        localConfig = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig);
        Class.forName("org.h2.Driver");
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

    protected Set<OLAPContext> collectAllOlapContexts(AbstractContext smartContext) {
        Preconditions.checkArgument(smartContext != null);
        Set<OLAPContext> olapContexts = Sets.newHashSet();
        final List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        modelContexts.forEach(modelCtx -> olapContexts.addAll(modelCtx.getModelTree().getOlapContexts()));

        return olapContexts;
    }
}
